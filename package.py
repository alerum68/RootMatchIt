import ast
import os
import astor
import black

files = [
    "functions/setup_logging.py",
    "functions/database.py",
    "classes/rm_classes.py",
    "classes/a_classes.py",
    "classes/ftdna_classes.py",
    "classes/mh_classes.py",
    "functions/select_kits.py",
    "functions/import_data.py",
]

import_lines = set()
imported_base = False


def extract_imports_and_base(filepath):
    # Function to extract import statements and base assignment from a file
    global imported_base

    with open(filepath, "r") as in_file:
        ast_tree = ast.parse(in_file.read(), filename=filepath)
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    import_lines.add(f"import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                for alias in node.names:
                    import_lines.add(f"from {module_name} import {alias.name}")
            elif isinstance(node, ast.Assign):
                if any(isinstance(target, ast.Name) and target.id == "Ancestry_Base" for target in node.targets):
                    if isinstance(node.value, ast.Call):
                        if isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "declarative_base":
                            if isinstance(node.value.func.value,
                                          ast.Name) and node.value.func.value.id == "sqlalchemy" and not imported_base:
                                import_lines.add("from sqlalchemy.ext.declarative import declarative_base as "
                                                 "Ancestry_Base")
                                imported_base = True


# Iterate over each file and extract imports and base assignment
for fname in files:
    file_path = os.path.join(os.getcwd(), fname)
    extract_imports_and_base(file_path)

# Write imports and file contents to DG2RM.py
with open("RootMatchIt.py", "w") as outfile:
    # Write unique imports
    outfile.write("\n".join(sorted(import_lines)) + "\n\n")

    # Write contents of each file
    for fname in files:
        file_path = os.path.join(os.getcwd(), fname)
        with open(file_path, "r") as infile:
            tree = ast.parse(infile.read(), filename=file_path)
            body = [node for node in tree.body if not isinstance(node, (ast.Import, ast.ImportFrom))]
            source_code = astor.to_source(ast.Module(body=body)).strip()
            reformatted_code = black.format_str(source_code, mode=black.FileMode()).strip()
            outfile.write("\n" + reformatted_code + "\n\n")

print("\nConsolidation complete. Output written to RootMatchIt.py. Check formatting before pushing to git.")
