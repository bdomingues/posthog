import { actions, afterMount, kea, key, listeners, path, props, reducers } from 'kea'
import { loaders } from 'kea-loaders'

import api from 'lib/api'
import { openPersonsModal } from 'scenes/trends/persons-modal/PersonsModal'

import {
    NodeKind,
    NpsBucket,
    ProductKey,
    SurveyResponseDriversActorsQuery,
    SurveyResponseDriversQuery,
    SurveyResponseDriversQueryResponse,
} from '~/queries/schema/schema-general'
import { setLatestVersionsOnQuery } from '~/queries/utils'

import { NPS_DETRACTOR_LABEL, NPS_PROMOTER_LABEL } from './constants'
import type { surveyResponseDriversLogicType } from './surveyResponseDriversLogicType'

export interface SurveyResponseDriversLogicProps {
    surveyId: string
}

export interface DriversPersonsModalPayload {
    event: string
    bucket: NpsBucket
    performed: boolean
}

// Single construction point for the table's source query: the drill-down MUST run
// against the same query the cells were computed from, or the modal can disagree
// with the cell the moment new query params (question picker, window) are added.
function driversSourceQuery(surveyId: string): SurveyResponseDriversQuery {
    return setLatestVersionsOnQuery<SurveyResponseDriversQuery>({
        kind: NodeKind.SurveyResponseDriversQuery,
        surveyId,
        tags: { productKey: ProductKey.SURVEYS },
    })
}

export const surveyResponseDriversLogic = kea<surveyResponseDriversLogicType>([
    props({} as SurveyResponseDriversLogicProps),
    key(({ surveyId }) => surveyId),
    path((key) => ['scenes', 'surveys', 'surveyResponseDriversLogic', key]),
    actions({
        openDriversPersonsModal: (payload: DriversPersonsModalPayload) => payload,
    }),
    loaders(({ props }) => ({
        driversResponse: {
            __default: null as SurveyResponseDriversQueryResponse | null,
            loadDrivers: async (): Promise<SurveyResponseDriversQueryResponse | null> => {
                return await api.query(driversSourceQuery(props.surveyId), { refresh: 'blocking' })
            },
        },
    })),
    reducers({
        errorLoading: [
            false,
            {
                loadDrivers: () => false,
                loadDriversSuccess: () => false,
                loadDriversFailure: () => true,
            },
        ],
    }),
    listeners(({ props }) => ({
        openDriversPersonsModal: ({ event, bucket, performed }) => {
            const query = setLatestVersionsOnQuery<SurveyResponseDriversActorsQuery>({
                kind: NodeKind.SurveyResponseDriversActorsQuery,
                source: driversSourceQuery(props.surveyId),
                event,
                bucket,
                performed,
            })
            const bucketLabel = bucket === 'detractor' ? NPS_DETRACTOR_LABEL : NPS_PROMOTER_LABEL
            openPersonsModal({
                title: (
                    <>
                        {bucketLabel} who {performed ? 'performed' : 'did not perform'} <b>{event}</b>
                    </>
                ),
                query,
            })
        },
    })),
    afterMount(({ actions }) => {
        actions.loadDrivers()
    }),
])
