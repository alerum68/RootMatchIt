import ast
import os

files = [
    "functions/setup_logging.py",
    "functions/database.py",
    "classes/rm_classes.py",
    "classes/a_classes.py",
    "classes/ftdna_classes.py",
    "classes/mh_classes.py",
    "functions/import_data.py",
    "functions/process_data.py",
    "functions/create_indices.py",
    "functions/main.py"
]

import_lines = set()
imported_base = False


def extract_imports_and_base(filepath):
    # Function to extract import statements and base assignment from a file
    global imported_base

    with open(filepath, "r") as in_file:
        tree = ast.parse(in_file.read(), filename=filepath)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    import_lines.add(f"import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                for alias in node.names:
                    import_lines.add(f"from {module_name} import {alias.name}")
            elif isinstance(node, ast.Assign):
                if any(isinstance(target, ast.Name) and target.id == "Base" for target in node.targets):
                    if isinstance(node.value, ast.Call):
                        if isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "declarative_base":
                            if module_name.startswith("sqlalchemy"):
                                if not imported_base:
                                    import_lines.add(f"from {module_name} import {node.value.func.attr} as Base")
                                    imported_base = True
                                else:
                                    continue


# Iterate over each file and extract imports and base assignment
for fname in files:
    file_path = os.path.join(os.getcwd(), fname)
    extract_imports_and_base(file_path)

# Write imports and file contents to DG2RM.py
with open("DG2RM.py", "w") as outfile:
    outfile.write("\n".join(sorted(import_lines)) + "\n\n")

    # Write contents of each file
    for fname in files:
        file_path = os.path.join(os.getcwd(), fname)
        with open(file_path, "r") as infile:
            # Skip writing imports from the individual files
            lines = infile.readlines()
            start_index = 0
            for i, line in enumerate(lines):
                if line.startswith("import ") or line.startswith("from "):
                    start_index = i + 1
                else:
                    break
            outfile.write(
                "\n" + "".join(lines[start_index:]).strip() + "\n\n")  # Write contents and ensure double newline

print("\n Consolidation complete. Output written to DG2RM.py. Check formating before pushing to git.")
