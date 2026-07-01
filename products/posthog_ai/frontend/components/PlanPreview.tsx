import { useState } from 'react'

import { IconChevronRight } from '@posthog/icons'
import { LemonButton } from '@posthog/lemon-ui'

import { MarkdownMessage } from '../messages/MarkdownMessage'

/** Pull the plan markdown out of an `ExitPlanMode` tool input, matching `/code`'s `toolCall.rawInput.plan`. */
export function getPlanText(input: Record<string, unknown> | undefined): string | undefined {
    const plan = input?.plan
    return typeof plan === 'string' && plan.trim() ? plan : undefined
}

export interface PlanPreviewProps {
    plan: string
    /** Stable id for the memoized markdown blocks. */
    id: string
    defaultExpanded?: boolean
}

/**
 * Expandable preview of the agent's plan (the `ExitPlanMode` payload), the parity counterpart to `/code`'s
 * `PlanContent` / `PlanApprovalView`. Renders the plan markdown behind a "Show plan / Hide plan" toggle so the
 * approval prompt stays scannable but the full plan is one click away.
 */
export function PlanPreview({ plan, id, defaultExpanded = false }: PlanPreviewProps): JSX.Element {
    const [expanded, setExpanded] = useState(defaultExpanded)

    return (
        <div className="flex flex-col gap-1">
            <LemonButton
                size="xsmall"
                icon={
                    <IconChevronRight
                        className={expanded ? 'rotate-90 transition-transform' : 'transition-transform'}
                    />
                }
                onClick={() => setExpanded(!expanded)}
                aria-expanded={expanded}
            >
                {expanded ? 'Hide plan' : 'Show plan'}
            </LemonButton>
            {expanded && (
                <div className="max-h-[50vh] overflow-y-auto">
                    <MarkdownMessage content={plan} id={id} />
                </div>
            )}
        </div>
    )
}
