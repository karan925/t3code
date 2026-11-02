import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";

export const PinnedFoldersState = Schema.Struct({
  folders: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      collapsed: Schema.Boolean,
    }),
  ),
  threadFolderByKey: Schema.Record(Schema.String, Schema.String),
  draftFolderById: Schema.Record(Schema.String, Schema.String).pipe(
    Schema.withDecodingDefault(Effect.succeed({})),
  ),
});
export type PinnedFoldersState = typeof PinnedFoldersState.Type;

export const EMPTY_PINNED_FOLDERS: PinnedFoldersState = {
  folders: [],
  threadFolderByKey: {},
  draftFolderById: {},
};

type FolderAction =
  | { type: "create"; id: string; name: string }
  | { type: "rename"; id: string; name: string }
  | { type: "delete"; id: string }
  | { type: "toggle"; id: string }
  | { type: "assign-draft"; draftId: string; folderId: string }
  | { type: "move"; threadKeys: readonly string[]; folderId: string | null };

export function updatePinnedFolders(
  state: PinnedFoldersState,
  action: FolderAction,
): PinnedFoldersState {
  switch (action.type) {
    case "create":
      if (!action.name.trim() || state.folders.some((folder) => folder.id === action.id))
        return state;
      return {
        ...state,
        folders: [...state.folders, { id: action.id, name: action.name.trim(), collapsed: false }],
      };
    case "rename":
      if (!action.name.trim()) return state;
      return {
        ...state,
        folders: state.folders.map((folder) =>
          folder.id === action.id ? { ...folder, name: action.name.trim() } : folder,
        ),
      };
    case "toggle":
      return {
        ...state,
        folders: state.folders.map((folder) =>
          folder.id === action.id ? { ...folder, collapsed: !folder.collapsed } : folder,
        ),
      };
    case "delete":
      return {
        ...state,
        folders: state.folders.filter((folder) => folder.id !== action.id),
        threadFolderByKey: Object.fromEntries(
          Object.entries(state.threadFolderByKey).filter(([, id]) => id !== action.id),
        ),
        draftFolderById: Object.fromEntries(
          Object.entries(state.draftFolderById).filter(([, id]) => id !== action.id),
        ),
      };
    case "assign-draft":
      if (!state.folders.some((folder) => folder.id === action.folderId)) return state;
      return {
        ...state,
        folders: state.folders.map((folder) =>
          folder.id === action.folderId ? { ...folder, collapsed: false } : folder,
        ),
        draftFolderById: { ...state.draftFolderById, [action.draftId]: action.folderId },
      };
    case "move": {
      if (
        action.folderId !== null &&
        !state.folders.some((folder) => folder.id === action.folderId)
      )
        return state;
      const threadFolderByKey = { ...state.threadFolderByKey };
      for (const key of action.threadKeys) {
        if (action.folderId === null) delete threadFolderByKey[key];
        else threadFolderByKey[key] = action.folderId;
      }
      return { ...state, threadFolderByKey };
    }
  }
}

export function groupPinnedThreads<T>(
  threads: readonly T[],
  state: PinnedFoldersState,
  keyOf: (thread: T) => string,
) {
  const byFolder = new Map<string, T[]>(state.folders.map((folder) => [folder.id, []]));
  const unfiled: T[] = [];
  for (const thread of threads) {
    const folderId = state.threadFolderByKey[keyOf(thread)];
    (folderId === undefined ? unfiled : (byFolder.get(folderId) ?? unfiled)).push(thread);
  }
  const groups = [
    { folder: null, threads: unfiled },
    ...state.folders.map((folder) => ({ folder, threads: byFolder.get(folder.id)! })),
  ];
  return groups.map((group) => ({
    ...group,
    visibleThreads: group.folder?.collapsed ? [] : group.threads,
  }));
}
