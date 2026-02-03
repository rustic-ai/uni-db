import glob
import json
import os
import sys


def run_notebook(notebook_path):
    print(f"Running {notebook_path}...")
    with open(notebook_path) as f:
        nb = json.load(f)

    code_cells = [cell for cell in nb["cells"] if cell["cell_type"] == "code"]

    # Concatenate code
    code = ""
    for cell in code_cells:
        source = "".join(cell["source"])
        code += source + "\n\n"

    # Execute
    try:
        # We need to set up the environment so imports work
        # The notebooks assume sys.path.append(".." works to find 'uni'
        # We are running this script likely from bindings/uni-db/examples or bindings/uni-db/
        # Let's adjust cwd to the notebook directory to match its perspective

        original_cwd = os.getcwd()
        os.chdir(os.path.dirname(os.path.abspath(notebook_path)))

        exec_globals = {}
        exec(code, exec_globals)

        os.chdir(original_cwd)
        print(f"SUCCESS: {notebook_path}")
        return True
    except Exception as e:
        print(f"FAILURE: {notebook_path}")
        print(e)
        import traceback

        traceback.print_exc()
        return False


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    notebooks = glob.glob(os.path.join(base_dir, "*.ipynb"))

    success = True
    for nb in notebooks:
        if not run_notebook(nb):
            success = False

    if success:
        print("\nAll notebooks ran successfully!")
        sys.exit(0)
    else:
        print("\nSome notebooks failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
