import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { threadEnvironment } from "../state/threads";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { useOrchestrationCommand } from "../state/use-orchestration-command";
import { placeSubmittedFolderThread } from "../sidebarPinnedFolderStorage";

export function usePinFolderThread() {
  const pin = useOrchestrationCommand(threadEnvironment.pin, { reportFailure: true });
  return async (folderId: string | null, ref: ScopedThreadRef) => {
    try {
      await placeSubmittedFolderThread(folderId, scopedThreadKey(ref), async () => {
        const result = await pin({
          environmentId: ref.environmentId,
          input: { threadId: ref.threadId },
        });
        return result._tag !== "Failure";
      });
    } catch (error) {
      // Folder organization must never turn a successful submission into a retry.
      console.error("Could not place submitted chat in its sidebar folder", error);
    }
  };
}
