import { getLocalStorageItem, writeLocalStorageValue } from "./hooks/useLocalStorage";
import {
  EMPTY_PINNED_FOLDERS,
  PinnedFoldersState,
  updatePinnedFolders,
} from "./sidebarPinnedFolders";

export const PINNED_FOLDERS_STORAGE_KEY = "t3code:sidebar:pinned-folders:v1";

// Capture the folder before awaiting submission: promotion can remove the draft,
// and a multi-model send can create more than one thread from the same draft.
export function submittedDraftFolder(draftId: string | null): string | null {
  if (!draftId) return null;
  try {
    const state = getLocalStorageItem(PINNED_FOLDERS_STORAGE_KEY, PinnedFoldersState);
    return state?.draftFolderById[draftId] ?? null;
  } catch (error) {
    console.error("Could not read draft sidebar folder", error);
    return null;
  }
}

export async function placeSubmittedFolderThread(
  folderId: string | null,
  threadKey: string,
  pin: () => Promise<boolean>,
): Promise<void> {
  if (!folderId) return;
  const state = getLocalStorageItem(PINNED_FOLDERS_STORAGE_KEY, PinnedFoldersState);
  if (!state?.folders.some((folder) => folder.id === folderId)) return;
  if (!(await pin())) return;
  writeLocalStorageValue(
    PINNED_FOLDERS_STORAGE_KEY,
    (current) => updatePinnedFolders(current, { type: "move", folderId, threadKeys: [threadKey] }),
    EMPTY_PINNED_FOLDERS,
    PinnedFoldersState,
  );
}
