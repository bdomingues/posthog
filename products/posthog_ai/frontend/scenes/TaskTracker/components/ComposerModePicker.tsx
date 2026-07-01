import { useState } from 'react'

import { IconCheckCircle, IconChevronDown, IconDocument, IconPencil, IconWarning } from '@posthog/icons'
import {
    Button,
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuLabel,
    DropdownMenuRadioGroup,
    DropdownMenuRadioItem,
    DropdownMenuTrigger,
} from '@posthog/quill-primitives'

// IconRobot is not exported from @posthog/icons — it lives only in the legacy lib icon set.
import { IconRobot } from 'lib/lemon-ui/icons'

import { getModeLabel, MODE_OPTIONS, type PermissionMode } from 'products/posthog_ai/frontend/utils/composerModes'
import { InitialPermissionModeEnumApi } from 'products/tasks/frontend/generated/api.schemas'

const MODE_ICONS: Record<PermissionMode, JSX.Element> = {
    [InitialPermissionModeEnumApi.Auto]: <IconRobot />,
    [InitialPermissionModeEnumApi.Default]: <IconPencil />,
    [InitialPermissionModeEnumApi.AcceptEdits]: <IconCheckCircle />,
    [InitialPermissionModeEnumApi.Plan]: <IconDocument />,
    [InitialPermissionModeEnumApi.BypassPermissions]: <IconWarning />,
}

export interface ComposerModePickerProps {
    selectedMode: PermissionMode
    onModeChange: (mode: PermissionMode) => void
}

/**
 * Controlled, logic-free permission-mode picker for a composer footer — the parity counterpart to `/code`'s
 * `ModeSelector`. The caller owns the selection and its side effects (the run composer syncs it to the running
 * agent at send time; the new-task composer seeds the first run with it). This only renders the dropdown and
 * reports changes up.
 */
export function ComposerModePicker({ selectedMode, onModeChange }: ComposerModePickerProps): JSX.Element {
    const [open, setOpen] = useState(false)

    return (
        <DropdownMenu open={open} onOpenChange={setOpen}>
            <DropdownMenuTrigger
                render={
                    <Button variant="outline" size="sm">
                        {MODE_ICONS[selectedMode]}
                        {getModeLabel(selectedMode)}
                        <IconChevronDown />
                    </Button>
                }
            />
            <DropdownMenuContent className="w-auto min-w-(--anchor-width) max-w-80">
                <DropdownMenuRadioGroup
                    value={selectedMode}
                    onValueChange={(value) => {
                        onModeChange(value as PermissionMode)
                        setOpen(false)
                    }}
                >
                    <DropdownMenuLabel>Mode</DropdownMenuLabel>
                    {MODE_OPTIONS.map((option) => (
                        <DropdownMenuRadioItem key={option.value} value={option.value}>
                            <div className="flex flex-col">
                                <span className="flex items-center gap-1.5">
                                    {MODE_ICONS[option.value]}
                                    {option.label}
                                </span>
                                <span className="text-xs text-secondary">{option.description}</span>
                            </div>
                        </DropdownMenuRadioItem>
                    ))}
                </DropdownMenuRadioGroup>
            </DropdownMenuContent>
        </DropdownMenu>
    )
}
