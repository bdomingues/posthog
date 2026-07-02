import { useActions, useValues } from 'kea'

import { LemonBanner, LemonSkeleton, LemonTable, LemonTableColumns, LemonTag } from '@posthog/lemon-ui'

import { humanFriendlyNumber, percentage } from 'lib/utils/numbers'
import { pluralize } from 'lib/utils/strings'
import { InsightEmptyState, InsightErrorState } from 'scenes/insights/EmptyStates'

import { SurveyResponseDriver } from '~/queries/schema/schema-general'

import { NPS_DETRACTOR_LABEL, NPS_PROMOTER_LABEL } from './constants'
import { surveyResponseDriversLogic } from './surveyResponseDriversLogic'

function strengthOf(record: SurveyResponseDriver): number {
    return record.odds_ratio < 1 ? 1 / record.odds_ratio : record.odds_ratio
}

export function SurveyResponseDrivers({ surveyId }: { surveyId: string }): JSX.Element {
    const { driversResponse, driversResponseLoading, errorLoading } = useValues(
        surveyResponseDriversLogic({ surveyId })
    )
    const { loadDrivers } = useActions(surveyResponseDriversLogic({ surveyId }))

    const results = driversResponse?.results ?? []
    const totals = driversResponse?.totals
    const suppressedEvents = driversResponse?.suppressedEvents ?? 0

    const columns: LemonTableColumns<SurveyResponseDriver> = [
        {
            title: 'Event',
            key: 'event',
            render: function RenderEvent(_, record) {
                return <span className="font-medium">{record.event}</span>
            },
        },
        {
            title: 'Correlated with',
            key: 'direction',
            align: 'center',
            tooltip: 'Which group was more likely to perform this event. Correlation, not causation.',
            render: function RenderDirection(_, record) {
                return (
                    <LemonTag type={record.direction === 'detractor' ? 'danger' : 'success'}>
                        {record.direction === 'detractor' ? NPS_DETRACTOR_LABEL : NPS_PROMOTER_LABEL}
                    </LemonTag>
                )
            },
        },
        {
            title: 'Detractors',
            key: 'detractors',
            align: 'center',
            tooltip: 'Share of detractors who performed this event at least once in the window.',
            render: function RenderDetractorRate(_, record) {
                const { detractors_with, detractors_without } = record.population
                const total = detractors_with + detractors_without
                return (
                    <div className="flex flex-col">
                        <span className="text-lg font-medium">
                            {total > 0 ? percentage(detractors_with / total, 0) : '—'}
                        </span>
                        <span className="text-secondary text-xs">
                            {humanFriendlyNumber(detractors_with)} of {humanFriendlyNumber(total)}
                        </span>
                    </div>
                )
            },
        },
        {
            title: 'Promoters',
            key: 'promoters',
            align: 'center',
            tooltip: 'Share of promoters who performed this event at least once in the window.',
            render: function RenderPromoterRate(_, record) {
                const { promoters_with, promoters_without } = record.population
                const total = promoters_with + promoters_without
                return (
                    <div className="flex flex-col">
                        <span className="text-lg font-medium">
                            {total > 0 ? percentage(promoters_with / total, 0) : '—'}
                        </span>
                        <span className="text-secondary text-xs">
                            {humanFriendlyNumber(promoters_with)} of {humanFriendlyNumber(total)}
                        </span>
                    </div>
                )
            },
        },
        {
            title: 'Strength',
            key: 'odds_ratio',
            align: 'center',
            tooltip:
                'How many times more likely the correlated group was to perform this event, based on the odds ratio (adjusted for small samples). Used to rank drivers.',
            sorter: (a, b) => strengthOf(a) - strengthOf(b),
            render: function RenderOddsRatio(_, record) {
                return <span className="text-lg font-medium">{humanFriendlyNumber(strengthOf(record), 1)}x</span>
            },
        },
        {
            title: 'Confidence',
            key: 'confidence',
            align: 'center',
            tooltip:
                'Low confidence means fewer sampled respondents performed this event than the minimum sample size.',
            render: function RenderConfidence(_, record) {
                return (
                    <LemonTag type={record.confidence === 'high' ? 'default' : 'warning'}>
                        {record.confidence === 'high' ? 'High' : 'Low'}
                    </LemonTag>
                )
            },
        },
    ]

    return (
        <div className="flex flex-col gap-4">
            {driversResponseLoading && !driversResponse ? (
                <LemonSkeleton className="h-4 w-96" />
            ) : totals ? (
                <div className="text-secondary">
                    Comparing behavior of {humanFriendlyNumber(totals.detractors)} detractors against{' '}
                    {humanFriendlyNumber(totals.promoters)} promoters ({humanFriendlyNumber(totals.passives)} passives
                    excluded from ratios).
                </div>
            ) : null}
            {driversResponse?.skewed && (
                <LemonBanner type="warning">
                    Promoter and detractor counts are heavily imbalanced, so odds ratios mostly reflect the imbalance.
                    Treat these results with caution until both groups grow.
                </LemonBanner>
            )}
            <LemonTable
                data-attr="survey-response-drivers-table"
                columns={columns}
                loading={driversResponseLoading}
                dataSource={results}
                rowKey="event"
                emptyState={
                    errorLoading ? (
                        <InsightErrorState excludeDetail onRetry={() => loadDrivers()} />
                    ) : (
                        <InsightEmptyState
                            heading="No response drivers yet"
                            detail="Once enough respondents of this survey also perform events, the behaviors that distinguish detractors from promoters show up here."
                        />
                    )
                }
                footer={
                    suppressedEvents > 0 ? (
                        <div className="flex items-center mt-2 mx-2">
                            <span className="text-muted text-xs">
                                {pluralize(suppressedEvents, 'event')} hidden — fewer than{' '}
                                {driversResponse?.sampleThreshold} sampled respondents performed{' '}
                                {suppressedEvents === 1 ? 'it' : 'them'}.
                            </span>
                        </div>
                    ) : undefined
                }
                expandable={{
                    noIndent: true,
                    expandedRowRender: function RenderExpandedRow(record) {
                        return (
                            <LemonTable
                                embedded
                                stealth
                                columns={[
                                    { dataIndex: 'row' },
                                    { title: record.event, dataIndex: 'with_event' },
                                    { title: `No ${record.event}`, dataIndex: 'without_event' },
                                ]}
                                dataSource={[
                                    {
                                        row: NPS_DETRACTOR_LABEL,
                                        with_event: record.population.detractors_with,
                                        without_event: record.population.detractors_without,
                                    },
                                    {
                                        row: NPS_PROMOTER_LABEL,
                                        with_event: record.population.promoters_with,
                                        without_event: record.population.promoters_without,
                                    },
                                ]}
                            />
                        )
                    },
                }}
            />
        </div>
    )
}
