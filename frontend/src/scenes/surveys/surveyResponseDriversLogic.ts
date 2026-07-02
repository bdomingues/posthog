import { afterMount, kea, key, path, props, reducers } from 'kea'
import { loaders } from 'kea-loaders'

import api from 'lib/api'

import {
    NodeKind,
    ProductKey,
    SurveyResponseDriversQuery,
    SurveyResponseDriversQueryResponse,
} from '~/queries/schema/schema-general'
import { setLatestVersionsOnQuery } from '~/queries/utils'

import type { surveyResponseDriversLogicType } from './surveyResponseDriversLogicType'

export interface SurveyResponseDriversLogicProps {
    surveyId: string
}

export const surveyResponseDriversLogic = kea<surveyResponseDriversLogicType>([
    props({} as SurveyResponseDriversLogicProps),
    key(({ surveyId }) => surveyId),
    path((key) => ['scenes', 'surveys', 'surveyResponseDriversLogic', key]),
    loaders(({ props }) => ({
        driversResponse: {
            __default: null as SurveyResponseDriversQueryResponse | null,
            loadDrivers: async (): Promise<SurveyResponseDriversQueryResponse | null> => {
                const query = setLatestVersionsOnQuery<SurveyResponseDriversQuery>({
                    kind: NodeKind.SurveyResponseDriversQuery,
                    surveyId: props.surveyId,
                    tags: { productKey: ProductKey.SURVEYS },
                })
                return await api.query(query, { refresh: 'blocking' })
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
    afterMount(({ actions }) => {
        actions.loadDrivers()
    }),
])
