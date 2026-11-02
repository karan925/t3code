import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { getLocalStorageItem, setLocalStorageItem } from "./hooks/useLocalStorage";
import {
  EMPTY_PINNED_FOLDERS,
  PinnedFoldersState,
  updatePinnedFolders,
} from "./sidebarPinnedFolders";
import {
  PINNED_FOLDERS_STORAGE_KEY,
  placeSubmittedFolderThread,
  submittedDraftFolder,
} from "./sidebarPinnedFolderStorage";

const read = () => getLocalStorageItem(PINNED_FOLDERS_STORAGE_KEY, PinnedFoldersState)!;
const decodeFolders = Schema.decodeSync(PinnedFoldersState);
const write = (state: PinnedFoldersState) =>
  setLocalStorageItem(PINNED_FOLDERS_STORAGE_KEY, state, PinnedFoldersState);

describe("folder chat creation", () => {
  beforeEach(() => {
    const folders = updatePinnedFolders(EMPTY_PINNED_FOLDERS, {
      type: "create",
      id: "work",
      name: "Work",
    });
    write(
      updatePinnedFolders(folders, { type: "assign-draft", draftId: "draft", folderId: "work" }),
    );
  });

  it("loads existing folders without losing their pinned chats", () => {
    expect(
      decodeFolders({
        folders: [{ id: "work", name: "Work", collapsed: true }],
        threadFolderByKey: { "env:chat": "work" },
      }),
    ).toEqual({
      folders: [{ id: "work", name: "Work", collapsed: true }],
      threadFolderByKey: { "env:chat": "work" },
      draftFolderById: {},
    });
  });

  it("opens the destination folder for a new draft, leaving other folders closed", () => {
    let state = updatePinnedFolders(read(), { type: "toggle", id: "work" });
    state = updatePinnedFolders(state, { type: "create", id: "other", name: "Other" });
    state = updatePinnedFolders(state, { type: "toggle", id: "other" });
    state = updatePinnedFolders(state, { type: "assign-draft", draftId: "new", folderId: "work" });
    expect(state.folders.map((folder) => folder.collapsed)).toEqual([false, true]);
    expect(state.draftFolderById.new).toBe("work");
  });

  it("pins all models using their actual environment-scoped thread identities", async () => {
    const folder = submittedDraftFolder("draft");
    const pin = vi.fn(async () => true);
    await Promise.all([
      placeSubmittedFolderThread(folder, "remote:actual-a", pin),
      placeSubmittedFolderThread(folder, "local:actual-b", pin),
    ]);
    expect(pin).toHaveBeenCalledTimes(2);
    expect(read().threadFolderByKey).toEqual({
      "remote:actual-a": "work",
      "local:actual-b": "work",
    });
  });

  it("does not file a chat if pinning fails", async () => {
    await placeSubmittedFolderThread(submittedDraftFolder("draft"), "env:chat", async () => false);
    expect(read().threadFolderByKey).toEqual({});
  });

  it("does not resurrect a folder deleted while a chat is being pinned", async () => {
    await placeSubmittedFolderThread(submittedDraftFolder("draft"), "env:chat", async () => {
      write(updatePinnedFolders(read(), { type: "delete", id: "work" }));
      return true;
    });
    expect(read()).toEqual(EMPTY_PINNED_FOLDERS);
  });

  it("leaves ordinary chats alone and forgets draft placement when a folder is deleted", async () => {
    const pin = vi.fn(async () => true);
    await placeSubmittedFolderThread(submittedDraftFolder("unfiled"), "env:chat", pin);
    expect(pin).not.toHaveBeenCalled();
    write(updatePinnedFolders(read(), { type: "delete", id: "work" }));
    expect(submittedDraftFolder("draft")).toBeNull();
  });
});
