import { CollapsibleSectionHeader } from "../ui/collapsible-section-header";
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { type ReactNode } from "react";
import { cn } from "~/lib/utils";
import {
  animateSidebarLayoutChanges,
  sidebarMarkerId,
  type SidebarListMarker,
} from "../Sidebar.logic";
import { SIDEBAR_SORTABLE_DRAG_DISABLED } from "./SidebarThreadRow";
import { SidebarThreadUndoNotice } from "./SidebarThreadUndoNotice";
import { ChevronDownIcon, LayersIcon, MoreHorizontalIcon, SquarePenIcon } from "lucide-react";
import { Button } from "../ui/button";

function SortableSidebarMarker(props: {
  marker: SidebarListMarker;
  className?: string;
  children?: ReactNode;
  "data-testid"?: string;
}) {
  const { setNodeRef, transform, transition } = useSortable({
    id: sidebarMarkerId(props.marker),
    disabled: SIDEBAR_SORTABLE_DRAG_DISABLED,
    animateLayoutChanges: animateSidebarLayoutChanges,
  });
  return (
    <li
      ref={setNodeRef}
      data-thread-selection-safe
      data-testid={props["data-testid"]}
      className={cn("list-none", props.className)}
      style={{
        transform: CSS.Translate.toString(transform),
        transition: props.marker.endsWith("-placeholder") ? "none" : transition,
        visibility: transform?.scaleY === 0 ? "hidden" : undefined,
      }}
    >
      {props.children}
    </li>
  );
}

export function SidebarPinnedFolderHeader(props: {
  id: string;
  name: string;
  count: number;
  expanded: boolean;
  isDropTarget: boolean;
  onToggle: () => void;
  onNewChat: () => void;
  canCreateChat: boolean;
  onContextMenu: (position: { x: number; y: number }) => void;
}) {
  return (
    <SortableSidebarMarker marker={`pinned-folder:${props.id}`} className="mx-0.5 h-8 min-w-0">
      <div
        data-sidebar-folder-id={props.id}
        className={cn(
          "flex min-w-0 items-center rounded-md",
          props.isDropTarget && "bg-primary/10 text-primary ring-1 ring-inset ring-primary/60",
        )}
        onContextMenu={(event) => {
          event.preventDefault();
          props.onContextMenu({ x: event.clientX, y: event.clientY });
        }}
      >
        <button
          type="button"
          aria-expanded={props.expanded}
          onClick={props.onToggle}
          className={cn(
            "flex h-8 min-w-0 flex-1 cursor-pointer items-center gap-2 rounded-md px-2 text-left text-xs font-medium hover:bg-sidebar-row-hover focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/70",
            props.isDropTarget ? "text-primary" : "text-sidebar-muted-foreground",
          )}
        >
          <ChevronDownIcon
            aria-hidden
            className={cn("size-3 shrink-0", !props.expanded && "-rotate-90")}
          />
          <LayersIcon aria-hidden className="size-3 shrink-0" />
          <span className="min-w-0 flex-1 truncate">{props.name}</span>
          <span className="shrink-0 font-normal tabular-nums">
            {props.isDropTarget ? "Move here" : props.count}
          </span>
        </button>
        <Button
          size="icon-micro"
          variant="ghost-muted"
          aria-label={`New chat in ${props.name}`}
          title={`New chat in ${props.name}`}
          disabled={!props.canCreateChat}
          onClick={props.onNewChat}
        >
          <SquarePenIcon aria-hidden className="size-3" />
        </Button>
        <Button
          size="icon-micro"
          variant="ghost-muted"
          aria-label={`Manage folder ${props.name}`}
          onClick={(event) => {
            const rect = event.currentTarget.getBoundingClientRect();
            props.onContextMenu({ x: rect.right, y: rect.bottom });
          }}
        >
          <MoreHorizontalIcon aria-hidden className="size-3" />
        </Button>
      </div>
    </SortableSidebarMarker>
  );
}

export function SidebarFolderDrafts(props: { id: string; children: ReactNode }) {
  return (
    <SortableSidebarMarker marker={`pinned-folder-drafts:${props.id}`}>
      <ul>{props.children}</ul>
    </SortableSidebarMarker>
  );
}

export function SidebarSectionPlaceholder(props: {
  marker: "active-placeholder" | "settled-placeholder";
  label: string;
  showHint: boolean;
  isDropTarget: boolean;
}) {
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className="relative mx-0.5 -mb-px h-0"
    >
      {props.showHint ? (
        <div
          className={cn(
            "absolute inset-x-0 top-0 flex h-9 items-center justify-center rounded-md border border-dashed border-sidebar-foreground/25 text-xs text-sidebar-foreground/80",
            props.isDropTarget && "border-primary/40 bg-primary/5 text-primary",
          )}
        >
          {props.label}
        </div>
      ) : null}
    </SortableSidebarMarker>
  );
}

export function SidebarDragBoundary(props: {
  marker: "pinned-header" | "pinned-divider";
  label: string;
  visible: boolean;
  isDropTarget: boolean;
}) {
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className="pointer-events-none relative mx-0.5 -mb-px h-0"
    >
      {props.visible ? (
        <div className="sidebar-drag-boundary-label absolute inset-x-2 top-1 flex h-4 items-center gap-2">
          <span
            className={cn(
              "shrink-0 text-xs font-medium",
              props.isDropTarget ? "text-primary" : "text-sidebar-foreground/80",
            )}
          >
            {props.label}
          </span>
          <span
            aria-hidden
            className={cn(
              "h-px flex-1",
              props.isDropTarget ? "bg-primary/50" : "bg-sidebar-foreground/25",
            )}
          />
        </div>
      ) : null}
    </SortableSidebarMarker>
  );
}

export function SidebarSectionHeader(props: {
  marker: "working-header" | "snoozed-header" | "settled-header";
  label: string;
  /** A muted trailing count. */
  count: number;
  className?: string;
  dragging?: boolean;
  isDropTarget?: boolean;
  toggle: { expanded: boolean; onToggle: () => void };
  /** The first shelf at the bottom of the list hosts the undo notice just above itself. */
  hostsUndoNotice?: boolean;
}) {
  const shelf =
    props.marker === "working-header"
      ? "working"
      : props.marker === "snoozed-header"
        ? "snoozed"
        : "settled";
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className={cn("relative mx-0.5 h-8", props.className)}
    >
      {props.hostsUndoNotice ? (
        <SidebarThreadUndoNotice className="absolute inset-x-0 bottom-full bg-sidebar surface-grain" />
      ) : null}
      <CollapsibleSectionHeader
        onClick={props.toggle.onToggle}
        expanded={props.toggle.expanded}
        tone={props.isDropTarget ? "accent" : props.dragging ? "emphasized" : "muted"}
        data-testid={`sidebar-${shelf}-shelf-toggle`}
        accessory={<span className="font-normal tabular-nums">{props.count}</span>}
      >
        {props.label}
      </CollapsibleSectionHeader>
    </SortableSidebarMarker>
  );
}
