import {
  EMPTY_PINNED_FOLDERS,
  PinnedFoldersState,
  updatePinnedFolders,
} from "../sidebarPinnedFolders";
import { useLocalStorage } from "./useLocalStorage";
import { PINNED_FOLDERS_STORAGE_KEY } from "../sidebarPinnedFolderStorage";

export function useSidebarPinnedFolders() {
  const [state, setState] = useLocalStorage(
    PINNED_FOLDERS_STORAGE_KEY,
    EMPTY_PINNED_FOLDERS,
    PinnedFoldersState,
  );
  const dispatch = (action: Parameters<typeof updatePinnedFolders>[1]) =>
    setState((current) => updatePinnedFolders(current, action));
  return { state, dispatch };
}
