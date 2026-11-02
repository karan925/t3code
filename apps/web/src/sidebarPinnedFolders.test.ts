import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  EMPTY_PINNED_FOLDERS,
  PinnedFoldersState,
  groupPinnedThreads,
  updatePinnedFolders,
} from "./sidebarPinnedFolders";

describe("local pinned folders", () => {
  const folders = updatePinnedFolders(EMPTY_PINNED_FOLDERS, {
    type: "create",
    id: "work",
    name: "  Work  ",
  });

  it("persists folders and environment-scoped assignments across reloads", () => {
    const state = updatePinnedFolders(folders, {
      type: "move",
      threadKeys: ["env-a:chat"],
      folderId: "work",
    });
    const codec = Schema.fromJsonString(PinnedFoldersState);
    const restored = Schema.decodeSync(codec)(Schema.encodeSync(codec)(state));
    expect(groupPinnedThreads(["env-a:chat", "env-b:chat"], restored, (key) => key)).toEqual([
      { folder: null, threads: ["env-b:chat"], visibleThreads: ["env-b:chat"] },
      {
        folder: { id: "work", name: "Work", collapsed: false },
        threads: ["env-a:chat"],
        visibleThreads: ["env-a:chat"],
      },
    ]);
  });

  it("deleting a folder releases its chats without removing other assignments", () => {
    let state = updatePinnedFolders(folders, { type: "create", id: "other", name: "Other" });
    state = updatePinnedFolders(state, { type: "move", threadKeys: ["a"], folderId: "work" });
    state = updatePinnedFolders(state, { type: "move", threadKeys: ["b"], folderId: "other" });
    state = updatePinnedFolders(state, { type: "delete", id: "work" });
    expect(state.threadFolderByKey).toEqual({ b: "other" });
    expect(groupPinnedThreads(["a", "b"], state, (key) => key)[0]?.threads).toEqual(["a"]);
  });

  it("supports renaming, collapsing, moving between folders, and moving out", () => {
    let state = updatePinnedFolders(folders, { type: "rename", id: "work", name: "Renamed" });
    state = updatePinnedFolders(state, { type: "toggle", id: "work" });
    state = updatePinnedFolders(state, { type: "move", threadKeys: ["a", "b"], folderId: "work" });
    state = updatePinnedFolders(state, { type: "move", threadKeys: ["a"], folderId: null });
    expect(state.folders).toEqual([{ id: "work", name: "Renamed", collapsed: true }]);
    expect(state.threadFolderByKey).toEqual({ b: "work" });
  });

  it("retains empty folders and the pinned order within each folder", () => {
    const state = updatePinnedFolders(folders, {
      type: "move",
      threadKeys: ["b", "a"],
      folderId: "work",
    });
    expect(groupPinnedThreads(["a", "c", "b"], state, (key) => key)[1]?.threads).toEqual([
      "a",
      "b",
    ]);
    expect(groupPinnedThreads([], state, (key: string) => key)[1]?.threads).toEqual([]);
  });

  it("collapses every chat, including the selected chat, while retaining the folder count", () => {
    let state = updatePinnedFolders(folders, {
      type: "move",
      threadKeys: ["selected"],
      folderId: "work",
    });
    state = updatePinnedFolders(state, { type: "toggle", id: "work" });
    const group = groupPinnedThreads(["selected", "unfiled"], state, (key) => key)[1];
    expect(group?.threads).toEqual(["selected"]);
    expect(group?.visibleThreads).toEqual([]);
  });

  it("keeps closed folders hidden while another folder is open", () => {
    let state = updatePinnedFolders(folders, { type: "move", threadKeys: ["a"], folderId: "work" });
    state = updatePinnedFolders(state, { type: "toggle", id: "work" });
    state = updatePinnedFolders(state, { type: "create", id: "other", name: "Other" });
    state = updatePinnedFolders(state, { type: "move", threadKeys: ["b"], folderId: "other" });
    const groups = groupPinnedThreads(["a", "b"], state, (key) => key);
    expect(groups[1]?.visibleThreads).toEqual([]);
    expect(groups[2]?.visibleThreads).toEqual(["b"]);
  });

  it("ignores invalid destinations, duplicate folders, and blank names", () => {
    expect(
      updatePinnedFolders(folders, { type: "move", threadKeys: ["a"], folderId: "missing" }),
    ).toBe(folders);
    expect(updatePinnedFolders(folders, { type: "create", id: "work", name: "Duplicate" })).toBe(
      folders,
    );
    expect(updatePinnedFolders(folders, { type: "rename", id: "work", name: " " })).toBe(folders);
    expect(updatePinnedFolders(folders, { type: "create", id: "empty", name: " " })).toBe(folders);
  });
});
