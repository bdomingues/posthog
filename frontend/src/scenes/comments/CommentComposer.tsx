import { useActions, useValues } from 'kea'
import { useEffect } from 'react'

import { LemonButton, LemonLabel } from '@posthog/lemon-ui'

import { humanizeScope } from 'lib/components/ActivityLog/humanizeActivity'
import { IntegrationChoice } from 'lib/components/CyclotronJob/integrations/IntegrationChoice'
import { FEATURE_FLAGS } from 'lib/constants'
import { integrationsLogic } from 'lib/integrations/integrationsLogic'
import { SlackChannelPicker, SlackNotConfiguredBanner } from 'lib/integrations/SlackIntegrationHelpers'
import { IconSlack } from 'lib/lemon-ui/icons'
import { LemonRichContentEditor } from 'lib/lemon-ui/LemonRichContent/LemonRichContentEditor'
import { featureFlagLogic } from 'lib/logic/featureFlagLogic'

import { KeyboardShortcut } from '~/layout/navigation-3000/components/KeyboardShortcut'

import { CommentsLogicProps, commentsLogic } from './commentsLogic'

export const CommentComposer = (props: CommentsLogicProps): JSX.Element => {
    const {
        key,
        commentsLoading,
        replyingCommentId,
        itemContext,
        isEmpty,
        composerSendToSlack,
        composerSlackIntegrationId,
        composerSlackChannel,
    } = useValues(commentsLogic(props))
    const {
        sendComposedContent,
        setReplyingComment,
        clearItemContext,
        setRichContentEditor,
        onRichContentEditorUpdate,
        setComposerSendToSlack,
        setComposerSlackIntegrationId,
        setComposerSlackChannel,
    } = useActions(commentsLogic(props))
    const { featureFlags } = useValues(featureFlagLogic)
    const { slackIntegrations } = useValues(integrationsLogic)

    // Toggling a brand-new top-level comment straight to Slack; replies sync automatically.
    const showSlackToggle = !replyingCommentId && !!featureFlags[FEATURE_FLAGS.DISCUSSIONS_SLACK_SYNC]
    const selectedIntegration = slackIntegrations?.find((integration) => integration.id === composerSlackIntegrationId)

    const placeholder = replyingCommentId
        ? 'Reply...'
        : `Comment on ${props.item_id ? 'this ' : ''}${humanizeScope(props.scope, !!props.item_id)}`

    useEffect(() => {
        // Whenever the discussion context changes or we fully unmount we clear the item context
        return () => clearItemContext()
        // oxlint-disable-next-line exhaustive-deps
    }, [key, clearItemContext])

    const primaryDisabledReason = isEmpty
        ? 'No message'
        : composerSendToSlack && !composerSlackIntegrationId
          ? 'Select a Slack workspace'
          : composerSendToSlack && !composerSlackChannel
            ? 'Select a Slack channel'
            : null

    return (
        <div className="deprecated-space-y-2">
            <LemonRichContentEditor
                key={key}
                logicKey="discussions"
                placeholder={placeholder}
                onCreate={setRichContentEditor}
                onUpdate={onRichContentEditorUpdate}
                onPressCmdEnter={() => sendComposedContent(false)}
                disabled={commentsLoading}
                footerActions={
                    showSlackToggle ? (
                        <LemonButton
                            size="small"
                            icon={<IconSlack />}
                            active={composerSendToSlack}
                            onClick={() => setComposerSendToSlack(!composerSendToSlack)}
                            tooltip="Send this comment to a Slack channel"
                            data-attr="discussions-comment-send-to-slack-toggle"
                        />
                    ) : null
                }
            />
            {composerSendToSlack ? (
                !slackIntegrations?.length ? (
                    <SlackNotConfiguredBanner />
                ) : (
                    <div className="flex flex-col gap-2 rounded border border-border p-2">
                        <div className="flex flex-col gap-1">
                            <LemonLabel>Slack workspace</LemonLabel>
                            <IntegrationChoice
                                integration="slack"
                                value={composerSlackIntegrationId ?? undefined}
                                onChange={(nextValue) => setComposerSlackIntegrationId(nextValue ?? null)}
                            />
                        </div>
                        {selectedIntegration ? (
                            <div className="flex flex-col gap-1">
                                <LemonLabel>Channel</LemonLabel>
                                <SlackChannelPicker
                                    value={composerSlackChannel ?? undefined}
                                    onChange={(nextValue) => setComposerSlackChannel(nextValue ?? null)}
                                    integration={selectedIntegration}
                                />
                            </div>
                        ) : null}
                    </div>
                )
            ) : null}
            <div className="flex justify-between items-center gap-2">
                <div className="flex-1" />
                {replyingCommentId ? (
                    <LemonButton type="secondary" onClick={() => setReplyingComment(null)}>
                        Cancel reply
                    </LemonButton>
                ) : null}
                {itemContext ? (
                    <LemonButton type="secondary" onClick={() => clearItemContext()}>
                        Cancel
                    </LemonButton>
                ) : null}
                {!replyingCommentId ? (
                    <LemonButton
                        type="secondary"
                        onClick={() => sendComposedContent(true)}
                        disabledReason={
                            composerSendToSlack
                                ? 'Turn off "Send to Slack" to add a task'
                                : isEmpty
                                  ? 'No message'
                                  : null
                        }
                        data-attr="discussions-comment-task"
                    >
                        Add as task
                    </LemonButton>
                ) : null}
                <LemonButton
                    type="primary"
                    onClick={() => sendComposedContent(false)}
                    // Guard against double-submit: posting (and the Slack send) runs through the
                    // comments loader, so commentsLoading disables the button while it's in flight.
                    loading={commentsLoading}
                    disabledReason={primaryDisabledReason}
                    sideIcon={<KeyboardShortcut command enter />}
                    data-attr="discussions-comment"
                >
                    {composerSendToSlack ? 'Send to Slack' : `Add ${replyingCommentId ? 'reply' : 'comment'}`}
                </LemonButton>
            </div>
        </div>
    )
}
