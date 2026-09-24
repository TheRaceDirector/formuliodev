import os
import shutil
from pathlib import Path

# SET TO False TO ACTUALLY DELETE FILES AND FOLDERS
DRY_RUN = False

# Target depth: 3 levels down from this script
# Example: root (0) / level_1 (1) / level_2 (2) / level_3_folder (3 - DELETED)
TARGET_DEPTH = 3


def run_cleanup():
    root_dir = Path(__file__).resolve().parent
    deleted_folders = 0
    deleted_csvs = 0

    print(f"--- Running cleanup in: {root_dir} ---")
    if DRY_RUN:
        print("[DRY RUN MODE] No files or folders will actually be deleted.\n")
    else:
        print("[LIVE MODE] Deleting matching files and directories...\n")

    for current_path, dirnames, filenames in os.walk(root_dir, topdown=True):
        current_rel = Path(current_path).relative_to(root_dir)
        depth = 0 if current_rel == Path(".") else len(current_rel.parts)

        # 1. Delete all CSV files encountered
        for filename in filenames:
            if filename.lower().endswith(".csv"):
                file_path = Path(current_path) / filename
                if DRY_RUN:
                    print(f"[Would Delete CSV]    {file_path.relative_to(root_dir)}")
                else:
                    file_path.unlink()
                    print(f"[Deleted CSV]         {file_path.relative_to(root_dir)}")
                deleted_csvs += 1

        # 2. Check for folders at depth 3
        # When current depth is 2, any folder in dirnames is at depth 3
        if depth == TARGET_DEPTH - 1:
            for dirname in list(dirnames):
                folder_path = Path(current_path) / dirname
                if DRY_RUN:
                    print(f"[Would Delete Folder] {folder_path.relative_to(root_dir)} (and all contents)")
                else:
                    shutil.rmtree(folder_path)
                    print(f"[Deleted Folder]      {folder_path.relative_to(root_dir)}")
                
                deleted_folders += 1
                # Prune dirnames so os.walk does not attempt to traverse into deleted folders
                dirnames.remove(dirname)

    mode_label = "simulated" if DRY_RUN else "completed"
    print(f"\nCleanup {mode_label}: {deleted_folders} folder(s), {deleted_csvs} CSV file(s).")
    if DRY_RUN:
        print("To execute real deletions, set `DRY_RUN = False` in the script.")


if __name__ == "__main__":
    run_cleanup()