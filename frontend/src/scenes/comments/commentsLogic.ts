import { actions, afterMount, connect, kea, key, listeners, path, props, reducers, selectors } from 'kea'
import { loaders } from 'kea-loaders'
import { subscriptions } from 'kea-subscriptions'

import { lemonToast } from '@posthog/lemon-ui'

import api from 'lib/api'
import { RichContentEditorType } from 'lib/components/RichContentEditor/types'
import { deleteWithUndo } from 'lib/utils/deleteWithUndo'
import { isEmptyObject } from 'lib/utils/guards'
import { membersLogic } from 'scenes/organization/membersLogic'
import { teamLogic } from 'scenes/teamLogic'
import { userLogic } from 'scenes/userLogic'

import { sidePanelDiscussionLogic } from '~/layout/navigation-3000/sidepanel/panels/discussion/sidePanelDiscussionLogic'
import { CommentType } from '~/types'

import { commentsSendToSlackCreate } from 'products/platform_features/frontend/generated/api'

import type { commentsLogicType } from './commentsLogicType'
import { sendCommentToSlackLogic } from './sendCommentToSlackLogic'
import { discussionsSlug, getTextContent } from './utils'

export type CommentsLogicProps = {
    scope: CommentType['scope']
    item_id?: CommentType['item_id']
    item_context?: CommentType['item_context']
    disabled?: boolean
}

export type CommentWithRepliesType = {
    id: CommentType['id']
    comment?: CommentType // It may have been deleted
    replies: CommentType[]
}

export type CommentContext = {
    context: Record<string, any> | null
    callback?: (event: { sent: boolean }) => void
}

export const commentsLogic = kea<commentsLogicType>([
    path(() => ['scenes', 'notebooks', 'Notebook', 'commentsLogic']),
    props({} as CommentsLogicProps),
    key((props) => `${props.scope}-${props.item_id || ''}`),

    connect(() => ({
        actions: [
            sidePanelDiscussionLogic,
            ['incrementCommentCount', 'scrollToLastComment'],
            membersLogic,
            ['ensureAllMembersLoaded'],
        ],
        values: [
            userLogic,
            ['user'],
            membersLogic,
            ['meFirstMembers'],
            teamLogic,
            ['currentProjectId', 'currentTeamId'],
        ],
    })),

    actions({
        loadComments: true,
        focusComposer: true,
        clearItemContext: true,
        maybeLoadComments: true,
        sendComposedContent: (asTask: boolean = false) => ({ asTask }),
        persistEditedComment: true,
        onRichContentEditorUpdate: (isEmpty: boolean) => ({ isEmpty }),
        onEditingCommentRichContentEditorUpdate: (isEmpty: boolean) => ({ isEmpty }),
        sendEmojiReaction: (emoji: string, sourceCommentId: string) => ({ emoji, sourceCommentId }),
        deleteComment: (comment: CommentType) => ({ comment }),
        completeComment: (comment: CommentType) => ({ comment }),
        reopenComment: (comment: CommentType) => ({ comment }),
        setEditingComment: (comment: CommentType | null) => ({ comment }),
        setReplyingComment: (commentId: string | null) => ({ commentId }),
        setSelectedComment: (commentId: string | null) => ({ commentId }),
        setCommentContexts: (contexts: Record<string, string>) => ({ contexts }),
        setItemContext: (context: Record<string, any> | null, callback?: (event: { sent: boolean }) => void) => ({
            context,
            callback,
        }),
        setRichContentEditor: (editor: RichContentEditorType) => ({ editor }),
        setEditingCommentRichContentEditor: (editor: RichContentEditorType | null) => ({ editor }),
        setComposerSendToSlack: (enabled: boolean) => ({ enabled }),
        setComposerSlackIntegrationId: (integrationId: number | null) => ({ integrationId }),
        setComposerSlackChannel: (channel: string | null) => ({ channel }),
    }),
    reducers({
        composerSendToSlack: [
            false,
            {
                setComposerSendToSlack: (_, { enabled }) => enabled,
                // Reset the toggle after a send so it doesn't stick across comments.
                sendComposedContentSuccess: () => false,
            },
        ],
        composerSlackIntegrationId: [
            null as number | null,
            {
                setComposerSlackIntegrationId: (_, { integrationId }) => integrationId,
                sendComposedContentSuccess: () => null,
            },
        ],
        composerSlackChannel: [
            null as string | null,
            {
                setComposerSlackChannel: (_, { channel }) => channel,
                // Picking a different workspace clears the channel.
                setComposerSlackIntegrationId: () => null,
                sendComposedContentSuccess: () => null,
            },
        ],
        replyingCommentId: [
            null as string | null,
            {
                setReplyingComment: (_, { commentId }) => commentId,
                sendComposedContentSuccess: () => null,
            },
        ],
        selectedCommentId: [
            null as string | null,
            {
                setSelectedComment: (_, { commentId }) => commentId,
                setReplyingComment: (state, { commentId }) => commentId ?? state,
                sendComposedContentSuccess: () => null,
            },
        ],
        commentContexts: [
            {} as Record<string, string>,
            {
                setCommentContexts: (_, { contexts }) => contexts,
            },
        ],
        itemContext: [
            null as CommentContext | null,
            {
                setItemContext: (_, itemContext) => (itemContext.context ? itemContext : null),
                sendComposedContentSuccess: () => null,
            },
        ],
        richContentEditor: [
            null as RichContentEditorType | null,
            {
                setRichContentEditor: (_, { editor }) => editor,
            },
        ],
        isEmpty: [
            true as boolean,
            {
                onRichContentEditorUpdate: (_, { isEmpty }) => isEmpty,
            },
        ],
        editingComment: [
            null as CommentType | null,
            {
                setEditingComment: (_, { comment }) => comment,
                persistEditedCommentSuccess: () => null,
            },
        ],
        editingCommentRichContentEditor: [
            null as RichContentEditorType | null,
            {
                setEditingCommentRichContentEditor: (_, { editor }) => editor,
                persistEditedCommentSuccess: () => null,
            },
        ],
        editingCommentExistingMentions: [
            null as number[] | null,
            {
                setEditingCommentRichContentEditor: (_, { editor }) => editor?.getMentions() ?? [],
                persistEditedCommentSuccess: () => null,
            },
        ],
        isEditingCommentEmpty: [
            false as boolean,
            {
                onEditingCommentRichContentEditorUpdate: (_, { isEmpty }) => isEmpty,
                persistEditedCommentSuccess: () => false,
            },
        ],
    }),
    loaders(({ props, values, actions }) => ({
        comments: [
            null as CommentType[] | null,
            {
                loadComments: async () => {
                    const response = await api.comments.list({
                        scope: props.scope,
                        item_id: props.item_id,
                    })

                    return response.results
                },
                sendComposedContent: async ({ asTask }) => {
                    const existingComments = values.comments ?? []

                    if (values.richContentEditor?.isEmpty()) {
                        console.error('Failed to create a comment because the content was empty')
                        return existingComments
                    }

                    let itemContext: Record<string, any> | undefined = {
                        ...values.itemContext?.context,
                        ...props.item_context,
                    }
                    if (isEmptyObject(itemContext)) {
                        itemContext = undefined
                    }

                    const mentions = values.richContentEditor?.getMentions() ?? []

                    const content = values.richContentEditor?.getJSON()

                    const textContent = getTextContent(content, values.meFirstMembers)

                    const composerAnchor = values.itemContext?.context
                    const isNewAnchoredThread =
                        composerAnchor && (composerAnchor.type === 'mark' || composerAnchor.type === 'node')

                    const isReply = !isNewAnchoredThread && !!values.replyingCommentId

                    const newComment = await api.comments.create({
                        rich_content: content,
                        content: textContent,
                        scope: props.scope,
                        item_id: props.item_id,
                        item_context: itemContext,
                        source_comment: isNewAnchoredThread ? undefined : (values.replyingCommentId ?? undefined),
                        mentions,
                        slug: discussionsSlug(props.scope, props.item_id),
                        is_task: asTask && !isReply,
                    })

                    values.itemContext?.callback?.({ sent: true })

                    // "Send to Slack" composer mode: mirror the new top-level comment to the chosen
                    // channel right away. Replies/tasks never mirror from here. The comment is created
                    // regardless; the Slack post is best-effort with its own toast.
                    const slackChannelId = values.composerSlackChannel?.split('|')[0]
                    if (
                        values.composerSendToSlack &&
                        !isReply &&
                        !asTask &&
                        values.composerSlackIntegrationId &&
                        slackChannelId &&
                        values.currentTeamId
                    ) {
                        try {
                            await commentsSendToSlackCreate(String(values.currentTeamId), newComment.id, {
                                integration_id: values.composerSlackIntegrationId,
                                channel_id: slackChannelId,
                            })
                            lemonToast.success('Discussion sent to Slack')
                            // Refetch and return the fresh list so the new comment shows its tracked-in-Slack
                            // state. We can't dispatch loadComments() here — it writes the same `comments`
                            // loader value this handler returns, and our return would supersede its result.
                            const response = await api.comments.list({ scope: props.scope, item_id: props.item_id })
                            return response.results
                        } catch {
                            lemonToast.error('Comment added, but sending to Slack failed')
                        }
                    }

                    return [...existingComments, newComment]
                },

                persistEditedComment: async () => {
                    const existingComments = values.comments ?? []
                    const editedComment = values.editingComment

                    if (!editedComment) {
                        return existingComments
                    }

                    const originalComment = existingComments.find((c) => c.id === editedComment.id)

                    if (!originalComment) {
                        return existingComments
                    }

                    const previousMentions = values.editingCommentExistingMentions ?? []
                    const currentMentions = values.editingCommentRichContentEditor?.getMentions() ?? []
                    const newMentions = currentMentions.filter((m) => !previousMentions.includes(m))

                    const { id, rich_content } = editedComment

                    const textContent = getTextContent(rich_content, values.meFirstMembers)

                    const updatedComment = await api.comments.update(id, {
                        rich_content,
                        content: textContent,
                        mentions: newMentions,
                        slug: discussionsSlug(props.scope, props.item_id),
                    })
                    return [...existingComments.filter((c) => c.id !== editedComment.id), updatedComment]
                },

                deleteComment: async ({ comment }) => {
                    await deleteWithUndo({
                        endpoint: `projects/${values.currentProjectId}/comments`,
                        object: { name: comment.item_context?.is_emoji ? 'Reaction' : 'Comment', id: comment.id },
                        callback: (isUndo) => {
                            if (isUndo) {
                                actions.loadCommentsSuccess([
                                    ...(values.comments?.filter((c) => c.id !== comment.id) ?? []),
                                    comment,
                                ])
                            }
                        },
                    })

                    return values.comments?.filter((c) => c.id !== comment.id) ?? null
                },

                sendEmojiReaction: async ({ emoji, sourceCommentId }) => {
                    const existingComments = values.comments ?? []

                    const newComment = await api.comments.create({
                        content: emoji,
                        scope: props.scope,
                        item_id: props.item_id,
                        source_comment: sourceCommentId,
                        item_context: {
                            is_emoji: true,
                        },
                        mentions: [],
                    })

                    return [...existingComments, newComment]
                },

                completeComment: async ({ comment }) => {
                    const updated = await api.comments.complete(comment.id)
                    const existing = values.comments ?? []
                    return existing.map((c) => (c.id === updated.id ? updated : c))
                },

                reopenComment: async ({ comment }) => {
                    const updated = await api.comments.reopen(comment.id)
                    const existing = values.comments ?? []
                    return existing.map((c) => (c.id === updated.id ? updated : c))
                },
            },
        ],
    })),

    listeners(({ values, actions }) => ({
        setReplyingComment: ({ commentId }) => {
            if (commentId) {
                actions.clearItemContext()
            }
        },
        clearItemContext: () => {
            values.itemContext?.callback?.({ sent: false })
            actions.setItemContext(null)
        },
        setItemContext: ({ context }) => {
            if (context) {
                values.richContentEditor?.focus()
            }
        },
        focusComposer: () => {
            values.richContentEditor?.focus()
        },
        maybeLoadComments: () => {
            if (!values.comments && !values.commentsLoading) {
                actions.loadComments()
            }
        },
        // After the ⋯-menu "Send to Slack" modal mirrors a comment, refresh so its tracked state
        // (the Slack icon) shows immediately instead of after a reload. The composer flow reloads
        // itself in the send loader.
        [sendCommentToSlackLogic.actionTypes.submitSuccess]: () => {
            actions.loadComments()
        },
        sendComposedContentSuccess: () => {
            actions.scrollToLastComment()
            actions.incrementCommentCount()
            values.richContentEditor?.clear()
        },
        loadCommentsSuccess: () => {
            actions.scrollToLastComment()
        },
    })),

    selectors({
        key: [() => [(_, props) => props], (props): string => `${props.scope}-${props.item_id || ''}`],
        sortedComments: [
            (s) => [s.comments],
            (comments) => {
                return comments?.sort((a, b) => (a.created_at > b.created_at ? 1 : -1)) ?? []
            },
        ],

        commentsWithReplies: [
            (s) => [s.sortedComments],
            (sortedComments) => {
                // NOTE: We build a tree of comments and replies here.
                // Comments may have been deleted so if we have a reply to a comment that no longer exists,
                // we still create the CommentWithRepliesType but with a null comment.

                const commentsById: Record<string, CommentWithRepliesType> = {}

                for (const comment of sortedComments ?? []) {
                    // Skip emoji reactions from the reply tree - they'll be handled separately
                    if (comment.item_context?.is_emoji) {
                        continue
                    }

                    let commentsWithReplies = commentsById[comment.source_comment ?? comment.id]

                    if (!commentsWithReplies) {
                        commentsById[comment.source_comment ?? comment.id] = commentsWithReplies = {
                            id: comment.source_comment ?? comment.id,
                            comment: undefined,
                            replies: [],
                        }
                    }

                    if (commentsWithReplies.id === comment.id) {
                        commentsWithReplies.comment = comment
                    } else {
                        commentsWithReplies.replies.push(comment)
                    }
                }

                return Object.values(commentsById)
            },
        ],

        emojiReactionsByComment: [
            (s) => [s.sortedComments],
            (sortedComments: CommentType[]) => {
                const reactions: Record<CommentType['id'], Record<string, CommentType[]>> = {}

                for (const comment of sortedComments ?? []) {
                    if (comment.item_context?.is_emoji && comment.source_comment) {
                        if (!reactions[comment.source_comment]) {
                            reactions[comment.source_comment] = {}
                        }
                        // TODO: emoji reactions still use the content field for now
                        const emoji = comment.content!
                        if (!reactions[comment.source_comment][emoji]) {
                            reactions[comment.source_comment][emoji] = []
                        }
                        reactions[comment.source_comment][emoji].push(comment)
                    }
                }

                return reactions
            },
        ],

        isMyComment: [
            (s) => [s.user],
            (user) => {
                return (comment: CommentType): boolean => comment.created_by?.uuid === user?.uuid
            },
        ],

        disabledReasonFor: [
            (s) => [s.user],
            (user) => {
                return (comment: CommentType): string | null =>
                    comment.created_by?.uuid === user?.uuid ? null : 'You can only delete your own comments'
            },
        ],
    }),

    subscriptions(({ actions }) => ({
        replyingCommentId: (value: string): void => {
            if (value) {
                actions.focusComposer()
            }
        },
    })),

    afterMount(({ actions }) => {
        actions.ensureAllMembersLoaded()
    }),
])
