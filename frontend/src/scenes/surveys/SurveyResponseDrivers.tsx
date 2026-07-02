import { useValues } from 'kea'

import { LemonBanner, LemonTable, LemonTableColumns, LemonTag } from '@posthog/lemon-ui'

import { humanFriendlyNumber, percentage } from 'lib/utils/numbers'
import { InsightEmptyState } from 'scenes/insights/EmptyStates'

import { SurveyResponseDriver } from '~/queries/schema/schema-general'
import { RatingSurveyQuestion, Survey, SurveyQuestionType } from '~/types'

import { surveyResponseDriversLogic } from './surveyResponseDriversLogic'

export function surveyHasNpsQuestion(survey: Pick<Survey, 'questions'>): boolean {
    return (survey.questions ?? []).some(
        (question) =>
            question.type === SurveyQuestionType.Rating &&
            (question as RatingSurveyQuestion).scale === 10 &&
            (question as RatingSurveyQuestion).isNpsQuestion !== false
    )
}

export function SurveyResponseDrivers({ surveyId }: { surveyId: string }): JSX.Element {
    const { driversResponse, driversResponseLoading } = useValues(surveyResponseDriversLogic({ surveyId }))

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
                        {record.direction === 'detractor' ? 'Detractors' : 'Promoters'}
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
                'Odds ratio between the two group shares, with a small-sample prior applied. Used to rank drivers.',
            sorter: (a, b) => a.odds_ratio - b.odds_ratio,
            render: function RenderOddsRatio(_, record) {
                const ratio = record.odds_ratio < 1 ? 1 / record.odds_ratio : record.odds_ratio
                return <span className="text-secondary">{humanFriendlyNumber(ratio, 1)}×</span>
            },
        },
        {
            title: 'Confidence',
            key: 'confidence',
            align: 'center',
            tooltip: 'Low confidence means fewer sampled responders performed this event than the minimum sample size.',
            render: function RenderConfidence(_, record) {
                return (
                    <LemonTag type={record.confidence === 'high' ? 'default' : 'caution'}>{record.confidence}</LemonTag>
                )
            },
        },
    ]

    return (
        <div className="flex flex-col gap-4">
            {totals && (
                <div className="text-secondary">
                    Comparing behavior of {humanFriendlyNumber(totals.detractors)} detractors against{' '}
                    {humanFriendlyNumber(totals.promoters)} promoters ({humanFriendlyNumber(totals.passives)} passives
                    excluded from ratios).
                </div>
            )}
            {driversResponse?.skewed && (
                <LemonBanner type="warning">
                    Promoter and detractor counts are heavily imbalanced, so odds ratios mostly reflect the imbalance.
                    Treat these results with caution until both groups grow.
                </LemonBanner>
            )}
            <LemonTable
                columns={columns}
                loading={driversResponseLoading}
                dataSource={results}
                emptyState={
                    <InsightEmptyState
                        heading="No response drivers yet"
                        detail="Once enough responders of this survey also perform events, the behaviors that distinguish detractors from promoters show up here."
                    />
                }
                expandable={{
                    noIndent: true,
                    expandedRowRender: function RenderExpandedRow(record) {
                        return (
                            <LemonTable
                                embedded
                                stealth
                                columns={[
                                    { title: '', dataIndex: 'row' },
                                    { title: `Did "${record.event}"`, dataIndex: 'with_event' },
                                    { title: `Did not`, dataIndex: 'without_event' },
                                ]}
                                dataSource={[
                                    {
                                        row: 'Detractors',
                                        with_event: record.population.detractors_with,
                                        without_event: record.population.detractors_without,
                                    },
                                    {
                                        row: 'Promoters',
                                        with_event: record.population.promoters_with,
                                        without_event: record.population.promoters_without,
                                    },
                                ]}
                            />
                        )
                    },
                }}
            />
            {suppressedEvents > 0 && (
                <div className="text-secondary text-xs">
                    {humanFriendlyNumber(suppressedEvents)} event{suppressedEvents === 1 ? '' : 's'} hidden — fewer than{' '}
                    {driversResponse?.sampleThreshold} sampled responders performed them, too few to report honestly.
                </div>
            )}
        </div>
    )
}
