#!/usr/bin/env python3
"""
Generate silver models for each Salesforce source instance by reading dbt_project.yml
and copying/replacing tokens in reusable silver templates.

Features:
- Robust parsing of vars.salesforce.source_instances supporting multiple YAML shapes
  (list of strings, dict of instances, and list of ad-hoc dicts like in user's example).
- Creates per-instance silver models with unique filenames (e.g., account__salesforce1.sql)
- Auto-generates consolidated silver models (e.g., account.sql) that UNION ALL
  references to per-instance models when multiple instances exist; if only one instance,
  the consolidated model just selects from that single ref.
- Safe token replacement using __INSTANCE__ in templates.

Usage:
    python generate_silver_models.py \
        --repo-root . \
        --project-yml dbt_project.yml \
        --templates-dir dbt_reusable/silver \
        --output-dir models/salesforce/silver \
        [--by-instance-subdir _by_instance]

Requires:
    PyYAML (install locally): pip install pyyaml
"""

import argparse
import sys
from pathlib import Path
import re
from typing import Dict, List, Any

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"YAML file not found: {path}")
    if yaml is None:
        raise RuntimeError(
            "PyYAML is not available. Please install locally with: pip install pyyaml"
        )
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        return data or {}



def _normalize_instances(si: Any) -> List[Dict[str, Any]]:
    """
    Normalizes vars.salesforce.source_instances into a list like:
        [{"name": "salesforce1", ...}, {"name": "salesforce2", ...}]
    Supports:
      1) list of strings: ['salesforce1', 'salesforce2']
      2) dict: {'salesforce1': {...}, 'salesforce2': {...}}
      3) list of single-key dicts whose value is dict OR list-of-dicts:
         - salesforce1:
           - source_database_name: salesforce_db
           - source_schema_name: salesforce1
           - tool_name: fivetran
    """
    instances: List[Dict[str, Any]] = []

    if isinstance(si, list):
        for item in si:
            if isinstance(item, str):
                instances.append({"name": item})
            elif isinstance(item, dict):
                if "name" in item:
                    inst = {"name": str(item.get("name"))}
                    inst.update({k: v for k, v in item.items() if k != "name"})
                    instances.append(inst)
                elif len(item) == 1:
                    # shape: {salesforce1: <dict or list of dicts>}
                    k = next(iter(item))
                    v = item[k]
                    inst = {"name": str(k)}
                    if isinstance(v, list):
                        # Merge the list of dicts into one flat dict
                        merged: Dict[str, Any] = {}
                        for sub in v:
                            if isinstance(sub, dict):
                                merged.update(sub)
                        inst.update(merged)
                    elif isinstance(v, dict):
                        inst.update(v)
                    instances.append(inst)
                else:
                    raise ValueError("Unrecognized instance item in list.")
            else:
                raise ValueError("source_instances list must contain strings or dicts.")

    elif isinstance(si, dict):
        for name, cfg in si.items():
            entry = {"name": str(name)}
            if isinstance(cfg, dict):
                entry.update(cfg)
            instances.append(entry)
    else:
        raise ValueError("vars.salesforce.source_instances must be a list or a dict.")

    # Deduplicate by name while preserving order
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for inst in instances:
        n = inst.get("name")
        if not n or n in seen:
            continue
        seen.add(n)
        deduped.append(inst)
    return deduped



def extract_salesforce_config(project: Dict[str, Any]) -> Dict[str, Any]:
    vars_dict = project.get("vars", {}) or {}
    salesforce = vars_dict.get("salesforce", {}) or {}

    defaults = {
        "source_database_name": salesforce.get("source_database_name") or vars_dict.get("source_database_name"),
        "source_schema_prefix": salesforce.get("source_schema_prefix") or vars_dict.get("source_schema_prefix"),
    }

    # Look for source_instances in salesforce first, then in vars root
    si = salesforce.get("source_instances") or vars_dict.get("source_instances")
    instances = _normalize_instances(si) if si is not None else []

    return {"source_instances": instances, "defaults": defaults}


def substitute_instance_token(text: str, instance_name: str) -> str:
    replaced = text.replace("__INSTANCE__", instance_name)
    replaced = re.sub(r"salesforce__\s*__INSTANCE__", f"salesforce__{instance_name}", replaced)
    return replaced


def add_ephemeral_materialization(text: str) -> str:
    """
    Add materialized='ephemeral' to the config block if it doesn't already exist.
    """
    if "materialized" in text:
        return text
    
    # Find the config block and add materialized='ephemeral'
    text = re.sub(
        r"(\{\{\s*config\s*\()",
        r"\1materialized='ephemeral', ",
        text
    )
    return text


def add_view_materialization(text: str) -> str:
    """
    Add materialized = 'view' to the config block if it doesn't already exist.
    """
    if "materialized" in text:
        return text
    
    # Find the config block and add materialized = 'view'
    text = re.sub(
        r"(\{\{\s*config\s*\()",
        r"\1materialized = 'view', ",
        text
    )
    return text


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)



def stem_from_template_name(filename: str) -> str:
    """
    Extract the stem from a template filename.
    E.g., 'account.sql.tpl' -> 'account', 'contact.sql' -> 'contact'
    """
    name = Path(filename).name
    # Remove common template extensions
    for ext in ['.sql.tpl', '.sql.tmpl', '.tpl', '.tmpl', '.sql']:
        if name.endswith(ext):
            return name[:-len(ext)]
    return name


def write_text(path: Path, content: str, overwrite: bool = False):
    """Write content to a file, with optional overwrite protection."""
    if path.exists() and not overwrite:
        raise FileExistsError(f"File already exists: {path}. Use --overwrite to replace it.")
    path.write_text(content, encoding="utf-8")
    print(f"[write] {path}")


def list_template_files(templates_dir: Path) -> List[Path]:
    # Support both .sql.tpl/.tpl and plain .sql as templates
    patterns = ["*.sql.tpl", "*.sql.tmpl", "*.tpl", "*.sql"]
    files: List[Path] = []
    for pat in patterns:
        files.extend(list(templates_dir.glob(pat)))
    return files


def copy_and_render_templates_per_instance(
    templates_dir: Path,
    out_dir: Path,
    instance_name: str,
    by_instance_subdir: str,
    overwrite: bool = False,
):
    # Debug: show what we see
    print(f"[debug] Probing templates in: {templates_dir}")
    files = list_template_files(templates_dir)
    print(f"[debug] Found {len(files)} template files: {[f.name for f in files]}")

    # Fallback: if empty, try sibling 'silver' of 'templates'
    if not files:
        alt = templates_dir.parent / "silver"
        if alt.exists():
            alt_files = list_template_files(alt)
            if alt_files:
                print(f"[info] No templates in {templates_dir}. Using fallback: {alt}")
                templates_dir = alt
                files = alt_files
                print(f"[debug] Found {len(files)} template files in fallback: {[f.name for f in files]}")

    if not files:
        raise FileNotFoundError(
            f"No template files found in {templates_dir}. "
            f"Expecting files like account.sql.tpl, contact.sql.tpl, or plain .sql."
        )

    inst_dir = out_dir / by_instance_subdir / instance_name
    ensure_dir(inst_dir)

    for tpl in files:
        content = tpl.read_text(encoding="utf-8")
        rendered = substitute_instance_token(content, instance_name)
        
        # Add materialized='ephemeral' for multiple instances
        rendered = add_ephemeral_materialization(rendered)

        base_name = tpl.name
        # normalize to .sql filename and add instance suffix to the stem
        # e.g., account.sql.tpl -> account__salesforce1.sql
        stem = stem_from_template_name(base_name)
        target_name = f"{stem}__{instance_name}.sql"
        target_path = inst_dir / target_name

        write_text(target_path, rendered, overwrite=overwrite)



def generate_consolidated_model(
    templates_dir: Path,
    out_dir: Path,
    instance_names: List[str],
    overwrite: bool = False,
):
    """
    Generate consolidated silver models that UNION ALL per-instance models
    while preserving config, comments, and structure.
    Adds materialized = 'view' for consolidated models.
    """

    if len(instance_names) <= 1:
        print("[info] Single instance detected. Skipping consolidation.")
        return

    files = list_template_files(templates_dir)

    # Deduplicate instance names and preserve order
    seen = set()
    deduped_instances: List[str] = []
    for i in instance_names:
        if i not in seen:
            seen.add(i)
            deduped_instances.append(i)

    for tpl in files:
        stem = stem_from_template_name(tpl.name)
        target_path = out_dir / f"{stem}.sql"

        template_text = tpl.read_text(encoding="utf-8")
        header = extract_header_until_config(template_text)
        # Ensure materialized = 'view' exists in the header (idempotent)
        header_with_materialization = add_view_materialization(header)

        union_lines = [f"    SELECT * FROM {{ ref('{stem}__{inst}') }}" for inst in deduped_instances]
        union_sql = "\n    UNION ALL\n".join(union_lines)

        consolidated_sql = f"""
{header_with_materialization}

WITH consolidated AS (
{union_sql}
)

SELECT *
FROM consolidated
"""

        # If the target exists, try to replace the existing consolidated CTE block
        if target_path.exists():
            existing = target_path.read_text(encoding="utf-8")
            updated, replaced = _replace_consolidated_block(existing, header_with_materialization, union_sql)
            if replaced:
                write_text(target_path, updated, overwrite=True)
            else:
                # No recognizable block to replace, overwrite with generated content
                write_text(target_path, consolidated_sql.strip(), overwrite=True)
        else:
            write_text(target_path, consolidated_sql.strip(), overwrite=overwrite)


def _replace_consolidated_block(existing_text: str, header_with_materialization: str, new_union_sql: str) -> (str, bool):
    """
    Replace the existing WITH consolidated AS ( ... ) block in `existing_text` with a new union built from
    `new_union_sql`. Returns (updated_text, replaced_flag).
    This keeps existing comments/header if possible and ensures idempotence.
    """
    # Build a regex that finds the consolidated CTE and the following SELECT * FROM consolidated
    pattern = re.compile(r"(WITH\s+consolidated\s+AS\s*\().*?(\)\s*SELECT\s+\*\s+FROM\s+consolidated)", re.DOTALL | re.IGNORECASE)

    replacement = f"WITH consolidated AS (\n{new_union_sql}\n)\n\nSELECT *\nFROM consolidated"

    if pattern.search(existing_text):
        updated = pattern.sub(replacement, existing_text)
        # Ensure header_with_materialization appears at the top; if not, prepend it
        if header_with_materialization.strip() and header_with_materialization.strip() not in updated:
            updated = header_with_materialization.rstrip() + "\n\n" + updated.lstrip()
        return updated, True

    return existing_text, False

def extract_header_until_config(template_text: str) -> str:
    """
    Extracts comments + config block only.
    """
    lines = template_text.splitlines()
    out = []
    in_config = False

    for line in lines:
        out.append(line)
        if "{{ config" in line:
            in_config = True
        elif in_config and "}}" in line:
            break

    return "\n".join(out)



    # The following code block was incorrectly indented and unreachable.
    # It should be part of generate_consolidated_model, not here.



def main():
    parser = argparse.ArgumentParser(description="Generate silver models per source instance + consolidated union models.")
    parser.add_argument("--repo-root", type=str, default=".", help="Path to the repository root.")
    parser.add_argument("--project-yml", type=str, default="dbt_project.yml", help="Path to dbt_project.yml.")
    parser.add_argument(
        "--templates-dir",
        type=str,
        default="models/salesforce/silver",
        help="Path to reusable silver templates.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="models/salesforce/silver",
        help="Destination for generated models.",
    )
    parser.add_argument(
        "--by-instance-subdir",
        type=str,
        default="_by_instance",
        help="Subdirectory under output-dir to store per-instance models.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files.")

    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    project_yml = (repo_root / args.project_yml).resolve()
    templates_dir = (repo_root / args.templates_dir).resolve()
    out_dir = (repo_root / args.output_dir).resolve()

    print(f"[info] Repo root:      {repo_root}")
    print(f"[info] Project YAML:   {project_yml}")
    print(f"[info] Templates dir:  {templates_dir}")
    print(f"[info] Output dir:     {out_dir}")

    project = load_yaml(project_yml)
    cfg = extract_salesforce_config(project)

    instances = cfg["source_instances"]
    if not instances:
        raise RuntimeError("No source instances found in dbt_project.yml under vars.salesforce.source_instances.")

    instance_names = [i["name"] for i in instances]
    print(f"[info] Instances detected: {', '.join(instance_names)}")

    # Ensure base output dirs
    ensure_dir(out_dir)

    # 1) Generate per-instance models (skip if only one instance)
    if len(instance_names) > 1:
        ensure_dir(out_dir / args.by_instance_subdir)
        for inst in instance_names:
            print(f"[info] Generating per-instance models for: {inst}")
            copy_and_render_templates_per_instance(
                templates_dir=templates_dir,
                out_dir=out_dir,
                instance_name=inst,
                by_instance_subdir=args.by_instance_subdir,
                overwrite=args.overwrite,
            )
    else:
        print(f"[info] Skipping per-instance model generation ({len(instance_names)} instance detected).")

   # 2) Generate consolidated union models ONLY if multiple instances exist
    if len(instance_names) > 1:
     print("[info] Multiple instances detected. Generating consolidated models (UNION ALL)...")
     generate_consolidated_model(
         templates_dir=templates_dir,
         out_dir=out_dir,
         instance_names=instance_names,
         overwrite=True,  # explicitly overwrite standard models
       )
    else:
     print(
        "[info] Single instance detected. "
        "Skipping consolidated model generation to preserve standard silver models."
    )


    print("[done] Silver model generation complete.")


if __name__ == "__main__":
    main()
