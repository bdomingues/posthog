import { actions, connect, kea, listeners, path, reducers, selectors } from 'kea'
import { urlToAction } from 'kea-router'

import { lemonToast } from '@posthog/lemon-ui'

import api from 'lib/api'
import { dayjs } from 'lib/dayjs'
import { urls } from 'scenes/urls'
import { userLogic } from 'scenes/userLogic'

import { Region, UserType } from '~/types'

import { adminLoginAs, loginAsFromTicket } from './adminLoginAs'
import type { impersonationNoticeLogicType } from './impersonationNoticeLogicType'

export interface ExpiredSessionInfo {
    email: string
    userId: number
    // Captured when the countdown fired so we can later confirm a fresh
    // /api/users/@me/ response represents an actual renewal, not the same
    // already-expired session echoing back.
    isImpersonatedUntil: string | null
}

export interface ImpersonationTicketContext {
    ticketId: string
    ticketNumber?: number
    email: string
    region?: Region
}

// Persisted across the post-impersonation page reload so the notice can offer a
// one-click return to the originating support ticket. We key on email so a stale
// value never attaches itself to an unrelated impersonation session.
export interface ReturnToTicketContext {
    ticketNumber: number
    email: string
}

export const impersonationNoticeLogic = kea<impersonationNoticeLogicType>([
    path(['layout', 'navigation', 'ImpersonationNotice', 'impersonationNoticeLogic']),

    connect(() => ({
        values: [userLogic, ['user', 'isImpersonationUpgradeInProgress']],
        actions: [
            userLogic,
            ['upgradeImpersonation', 'upgradeImpersonationSuccess', 'loadUser', 'loadUserSuccess', 'logout'],
        ],
    })),

    actions({
        minimize: true,
        maximize: true,
        openUpgradeModal: true,
        closeUpgradeModal: true,
        setPageVisible: (visible: boolean) => ({ visible }),
        clearPageHiddenAt: true,
        setTicketContext: (context: ImpersonationTicketContext | null) => ({ context }),
        initiateImpersonation: true,
        initiateImpersonationComplete: true,
        setSessionExpired: (info: ExpiredSessionInfo | null) => ({ info }),
        reImpersonate: (reason: string, readOnly: boolean) => ({ reason, readOnly }),
        reImpersonateFailure: (error: string) => ({ error }),
        returnToPostHog: true,
        setReturnToTicketContext: (context: ReturnToTicketContext | null) => ({ context }),
        returnToTicket: true,
    }),

    reducers({
        isMinimized: [
            false,
            {
                minimize: () => true,
                maximize: () => false,
            },
        ],
        isUpgradeModalOpen: [
            false,
            {
                openUpgradeModal: () => true,
                closeUpgradeModal: () => false,
            },
        ],
        pageHiddenAt: [
            null as number | null,
            {
                // Store timestamp when page becomes hidden - used to work out if we
                // should auto expand when page regains focus
                setPageVisible: (state, { visible }) => (visible ? state : Date.now()),
                clearPageHiddenAt: () => null,
            },
        ],
        ticketContext: [
            null as ImpersonationTicketContext | null,
            {
                setTicketContext: (_, { context }) => context,
            },
        ],
        isInitiatingImpersonation: [
            false,
            {
                initiateImpersonation: () => true,
                initiateImpersonationComplete: () => false,
            },
        ],
        expiredSessionInfo: [
            null as ExpiredSessionInfo | null,
            {
                setSessionExpired: (_, { info }) => info,
            },
        ],
        isReImpersonating: [
            false,
            {
                reImpersonate: () => true,
                reImpersonateFailure: () => false,
                setSessionExpired: () => false,
            },
        ],
        returnToTicketContext: [
            null as ReturnToTicketContext | null,
            { persist: true },
            {
                setReturnToTicketContext: (_, { context }) => context,
            },
        ],
        isReturningToTicket: [
            false,
            {
                returnToTicket: () => true,
            },
        ],
    }),

    selectors({
        isReadOnly: [(s) => [s.user], (user: UserType | null): boolean => user?.is_impersonated_read_only ?? true],
        isImpersonated: [(s) => [s.user], (user: UserType | null): boolean => user?.is_impersonated ?? false],
        isSessionExpired: [(s) => [s.expiredSessionInfo], (info: ExpiredSessionInfo | null): boolean => info !== null],
        canReturnToTicket: [
            (s) => [s.user, s.returnToTicketContext],
            (user: UserType | null, context: ReturnToTicketContext | null): boolean =>
                context !== null && (user?.is_impersonated ?? false) && user?.email === context.email,
        ],
        // The expired session belongs to a ticket impersonation if its email matches
        // the stored ticket context — the live user may already be gone by then.
        expiredSessionFromTicket: [
            (s) => [s.expiredSessionInfo, s.returnToTicketContext],
            (expired: ExpiredSessionInfo | null, context: ReturnToTicketContext | null): boolean =>
                expired !== null && context !== null && expired.email === context.email,
        ],
        returnTicketLabel: [
            (s) => [s.returnToTicketContext],
            (context: ReturnToTicketContext | null): string | null =>
                context ? `Return to ticket #${context.ticketNumber}` : null,
        ],
        returnTicketReason: [
            (s) => [s.returnToTicketContext],
            (context: ReturnToTicketContext | null): string =>
                context ? `Investigating ticket #${context.ticketNumber}` : '',
        ],
    }),

    listeners(({ actions, values }) => ({
        returnToPostHog: () => {
            // Restore the original staff login (via the loginas logout endpoint) and
            // land back in the PostHog app rather than the Django admin.
            actions.setReturnToTicketContext(null)
            window.location.href = `/admin/logout/?next=${encodeURIComponent('/')}`
        },
        returnToTicket: () => {
            const { returnToTicketContext } = values
            if (!returnToTicketContext) {
                return
            }
            // Returning to the ticket means dropping the customer impersonation and
            // landing back on the support ticket as the original staff user.
            const next = urls.supportTicketDetail(returnToTicketContext.ticketNumber)
            window.location.href = `/admin/logout/?next=${encodeURIComponent(next)}`
        },
        logout: () => {
            // A plain logout abandons impersonation entirely; drop the stored ticket
            // so it can't reattach to a later session for the same customer email.
            actions.setReturnToTicketContext(null)
        },
        initiateImpersonation: async () => {
            const { ticketContext } = values
            if (!ticketContext) {
                actions.initiateImpersonationComplete()
                return
            }

            try {
                const result = await loginAsFromTicket(ticketContext.ticketId)
                if (result.redirect_url) {
                    // Ticket belongs to another region — open that region's admin instead.
                    lemonToast.info(`This ticket is from ${result.redirect_region}. Opening in a new tab…`)
                    window.open(result.redirect_url, '_blank')
                    actions.initiateImpersonationComplete()
                    return
                }
                // Staying in this tab as the impersonated customer — remember the
                // ticket so the notice can offer a one-click return after the reload.
                if (ticketContext.ticketNumber != null) {
                    actions.setReturnToTicketContext({
                        ticketNumber: ticketContext.ticketNumber,
                        email: ticketContext.email,
                    })
                }
                // Reload into the app as the impersonated customer.
                window.location.replace('/')
            } catch (e) {
                lemonToast.error(e instanceof Error ? e.message : 'Failed to impersonate user')
                actions.initiateImpersonationComplete()
            }
        },
        upgradeImpersonationSuccess: () => {
            if (values.isUpgradeModalOpen && !values.isReadOnly) {
                actions.closeUpgradeModal()
            }
        },
        reImpersonate: async ({ reason, readOnly }) => {
            const { expiredSessionInfo } = values
            if (!expiredSessionInfo) {
                return
            }

            try {
                await adminLoginAs({ userId: expiredSessionInfo.userId, reason, readOnly })
                actions.loadUser()
            } catch (e) {
                const errorMessage = e instanceof Error ? e.message : 'Failed to re-impersonate'
                lemonToast.error(errorMessage)
                actions.reImpersonateFailure(errorMessage)
            }
        },
        loadUserSuccess: ({ user }) => {
            // Keep the stored ticket only while we're still impersonating the customer it
            // belongs to. Any other load — back to staff, or impersonating someone else —
            // means it has outlived its session, so drop it before it can reattach to an
            // unrelated impersonation. A same-customer re-impersonation by a non-ticket
            // path is indistinguishable here (we have no server-side signal tying an
            // impersonation to a ticket) and will still match.
            const ticket = values.returnToTicketContext
            if (ticket && !(user?.is_impersonated && user.email === ticket.email)) {
                actions.setReturnToTicketContext(null)
            }
            const { expiredSessionInfo } = values
            if (!expiredSessionInfo) {
                return
            }
            if (!user?.is_impersonated || !user.is_impersonated_until) {
                return
            }
            // Only dismiss if the fresh `is_impersonated_until` is strictly after the
            // one we saw when the countdown fired — otherwise the server is echoing
            // back the same stale session that already expired.
            const newUntil = dayjs(user.is_impersonated_until)
            const renewed = expiredSessionInfo.isImpersonatedUntil
                ? newUntil.isAfter(expiredSessionInfo.isImpersonatedUntil)
                : newUntil.isAfter(dayjs())
            if (renewed) {
                actions.setSessionExpired(null)
                lemonToast.success('Impersonation session renewed')
            }
        },
        setPageVisible: async ({ visible }) => {
            if (!visible) {
                return
            }
            if (values.expiredSessionInfo) {
                // Probe /api/users/@me/ directly rather than dispatching loadUser:
                // loadUser's failure path sets user=null, which would unmount the
                // app and the overlay along with it. On success we hand the fetched
                // user to loadUserSuccess ourselves so userLogic stays in sync.
                try {
                    const freshUser = await api.get<UserType>('api/users/@me/')
                    if (freshUser?.is_impersonated) {
                        actions.loadUserSuccess(freshUser)
                    }
                } catch {
                    // 401 or network error — overlay stays; user will pick an action.
                }
            }
            const { pageHiddenAt } = values
            actions.clearPageHiddenAt()
            // Auto-maximize when window regains focus to ensure staff
            // users are reminded they are impersonating a customer
            // Only trigger if away for more than 30 seconds though to
            // avoid being annoying if quickly switching between windows
            if (values.isMinimized && pageHiddenAt) {
                const secondsAway = (Date.now() - pageHiddenAt) / 1000
                if (secondsAway > 30) {
                    actions.maximize()
                }
            }
        },
    })),

    urlToAction(({ actions, values }) => ({
        '*': (_params, _searchParams, _hashParams, { pathname }) => {
            if (values.ticketContext && !pathname.startsWith('/support/tickets/')) {
                actions.setTicketContext(null)
            }
        },
    })),
])
