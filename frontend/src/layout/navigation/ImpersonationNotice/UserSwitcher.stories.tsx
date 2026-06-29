import { MOCK_DEFAULT_ORGANIZATION_MEMBER } from 'lib/api.mock'

import { Meta, StoryObj } from '@storybook/react'

import { OrganizationMembershipLevel } from 'lib/constants'
import { LemonMenuOverlay } from 'lib/lemon-ui/LemonMenu/LemonMenu'

import { OrganizationMemberType } from '~/types'

import { ChangeUserMenuItemLabel } from './ImpersonationNotice'

const meta: Meta = {
    title: 'Layout/Impersonation User Switcher',
    parameters: {
        layout: 'padded',
        viewMode: 'story',
    },
}
export default meta

const noop = (): void => {}

function member(
    firstName: string,
    lastName: string,
    email: string,
    level: OrganizationMembershipLevel,
    id: number
): OrganizationMemberType {
    return {
        ...MOCK_DEFAULT_ORGANIZATION_MEMBER,
        id: `member-${id}`,
        level,
        user: {
            ...MOCK_DEFAULT_ORGANIZATION_MEMBER.user,
            id,
            uuid: `uuid-${id}`,
            first_name: firstName,
            last_name: lastName,
            email,
        },
    }
}

// Matches the change-user list order: power desc (Owner → Admin → Member), names A→Z within a group.
const MEMBERS: OrganizationMemberType[] = [
    member('Anna', 'Ferreira', 'anna.ferreira@hedgebox.com', OrganizationMembershipLevel.Owner, 1),
    member('Zara', 'Okafor', 'zara@hedgebox.com', OrganizationMembershipLevel.Owner, 2),
    member('Bob', 'Stevenson', 'bob.the.administrator.stevenson@hedgebox.com', OrganizationMembershipLevel.Admin, 3),
    member('Cara', 'Liu', 'cara@hedgebox.com', OrganizationMembershipLevel.Member, 4),
]

export const UserSwitcherList: StoryObj = {
    render: () => (
        <div className="bg-surface-primary border rounded w-80">
            <LemonMenuOverlay
                items={MEMBERS.map((m) => ({
                    key: m.user.uuid,
                    label: <ChangeUserMenuItemLabel member={m} />,
                    onClick: noop,
                }))}
            />
        </div>
    ),
}
