import { useId, useState } from "react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogPanel,
  DialogFooter,
} from "../ui/dialog";

export function PinnedFolderDialog({
  initialName,
  onSave,
  onClose,
}: {
  initialName: string | null;
  onSave: (name: string) => void;
  onClose: () => void;
}) {
  const id = useId();
  const [name, setName] = useState(initialName ?? "");
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogPopup className="sm:max-w-sm">
        <form
          className="flex min-h-0 flex-col"
          onSubmit={(event) => {
            event.preventDefault();
            if (name.trim()) onSave(name.trim());
          }}
        >
          <DialogHeader>
            <DialogTitle>
              {initialName === null ? "New pinned folder" : "Rename folder"}
            </DialogTitle>
            <DialogDescription>Organize pinned chats locally on this device.</DialogDescription>
          </DialogHeader>
          <DialogPanel>
            <Label htmlFor={id}>Folder name</Label>
            <Input
              id={id}
              value={name}
              onChange={(event) => setName(event.target.value)}
              autoFocus
              maxLength={100}
            />
          </DialogPanel>
          <DialogFooter>
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" disabled={!name.trim()}>
              Save
            </Button>
          </DialogFooter>
        </form>
      </DialogPopup>
    </Dialog>
  );
}
