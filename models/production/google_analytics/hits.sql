SELECT
    hits.dataSource,
    hits.device,
    COALESCE(
        documents.doc_id,
        REGEXP_EXTRACT(hits.urlPath, '-(\\d{4,8})$')
    ) AS docId,
    hits.eventInfo,
    hits.geoNetwork,
    hits.hitHour,
    hits.hitId,
    hits.hitNumber,
    hits.hitTime,
    hits.hitType,
    hits.isInteraction,
    hits.page,
    hits.sessionId,
    hits.trafficSource,
    hits.urlPath,
    hits.urlQuery,
    hits.customDim1,
    hits.customDim2,
    hits.customDim3,
    hits.customDim4,
    hits.customDim5,
    hits.customDim6,
    hits.customDim7,
    hits.customDim8,
    hits.customDim9,
    hits.customDim10,
    hits.customDim11,
    hits.customDim12,
    hits.customDim13,
    hits.customDim14,
    hits.customDim15,
    hits.customDim16,
    hits.customDim17,
    hits.customDim18,
    hits.customDim19,
    hits.customDim20,
    hits.dt,
    CASE
        WHEN LENGTH(landingPage) >= 1000 THEN NULL
        ELSE landingPage
    END AS landingPage,
    CASE
        WHEN referralUrl LIKE 'http%' OR referralUrl LIKE 'android-app://%' THEN referralUrl
        ELSE NULL
    END AS referralUrl,
    CASE
        WHEN hits.hitNumber = hits.firstHitNumber THEN TRUE
        ELSE FALSE
    END AS isFirstHit,
    CASE
        WHEN hits.hitNumber = hits.firstPageHitNumber THEN TRUE
        ELSE FALSE
    END AS isFirstPageHit,
    CASE
        WHEN hits.hitNumber = hits.lastPageHitNumber THEN TRUE
        ELSE FALSE
    END AS isLastPageHit,
    top.timeOnPage
FROM (
    SELECT
        hits.dataSource,
        sessions.device,
        hits.eventInfo,
        sessions.geoNetwork,
        hits.hour AS hitHour,
        CONCAT(sessions.fullVisitorId, '_', CAST(sessions.visitId AS STRING), '_', hits.hitNumber) AS hitId,
        hits.hitNumber,
        hits.time AS hitTime,
        hits.type AS hitType,
        hits.isInteraction,
        (SELECT h.hitNumber FROM UNNEST(sessions.hits) h ORDER BY h.hitNumber ASC LIMIT 1) AS firstHitNumber,
        (SELECT h.hitNumber FROM UNNEST(sessions.hits) h ORDER BY h.hitNumber DESC LIMIT 1) AS lastHitNumber,
        (SELECT h.hitNumber FROM UNNEST(sessions.hits) h WHERE h.type = 'PAGE' ORDER BY h.hitNumber ASC LIMIT 1) AS firstPageHitNumber,
        (SELECT h.hitNumber FROM UNNEST(sessions.hits) h WHERE h.type = 'PAGE' ORDER BY h.hitNumber DESC LIMIT 1) AS lastPageHitNumber,
        CONCAT(
            'https://',
            CASE
                WHEN hits.page.pagePath LIKE '/%' THEN (SELECT page.hostname FROM UNNEST(sessions.hits) h3 WHERE h3.type = 'PAGE' ORDER BY h3.hitNumber ASC LIMIT 1)
                ELSE ''
            END,
            (SELECT page.pagePath FROM UNNEST(sessions.hits) h3 WHERE h3.type = 'PAGE' ORDER BY h3.hitNumber ASC LIMIT 1)
        ) AS landingPage,
        hits.page,
        (SELECT (SELECT value FROM h4.customdimensions WHERE index = 48) AS cd48 FROM UNNEST(sessions.hits) h4 ORDER BY h4.hitNumber ASC LIMIT 1) AS referralUrl,
        CONCAT(sessions.fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionId,
        sessions.trafficSource,
        CONCAT(
            'https://',
            CASE
                WHEN hits.page.pagePath LIKE '/%' THEN hits.page.hostname
                ELSE ''
            END,
            REGEXP_EXTRACT(hits.page.pagePath, "([^?^#]+)[?#]*.*") /* remove querystring and fragments */
        ) AS urlPath,
        REGEXP_EXTRACT(hits.page.pagePath, "[^\?]+[\?]([^#]*)[#]*.*") AS urlQuery,
        (SELECT value FROM hits.customdimensions WHERE index = 1 LIMIT 1) AS customDim1,
        (SELECT value FROM hits.customdimensions WHERE index = 2 LIMIT 1) AS customDim2,
        (SELECT value FROM hits.customdimensions WHERE index = 3 LIMIT 1) AS customDim3,
        (SELECT value FROM hits.customdimensions WHERE index = 4 LIMIT 1) AS customDim4,
        (SELECT value FROM hits.customdimensions WHERE index = 5 LIMIT 1) AS customDim5,
        (SELECT value FROM hits.customdimensions WHERE index = 6 LIMIT 1) AS customDim6,
        (SELECT value FROM hits.customdimensions WHERE index = 7 LIMIT 1) AS customDim7,
        (SELECT value FROM hits.customdimensions WHERE index = 8 LIMIT 1) AS customDim8,
        (SELECT value FROM hits.customdimensions WHERE index = 9 LIMIT 1) AS customDim9,
        (SELECT value FROM hits.customdimensions WHERE index = 10 LIMIT 1) AS customDim10,
        (SELECT value FROM hits.customdimensions WHERE index = 11 LIMIT 1) AS customDim11,
        (SELECT value FROM hits.customdimensions WHERE index = 12 LIMIT 1) AS customDim12,
        (SELECT value FROM hits.customdimensions WHERE index = 13 LIMIT 1) AS customDim13,
        (SELECT value FROM hits.customdimensions WHERE index = 14 LIMIT 1) AS customDim14,
        (SELECT value FROM hits.customdimensions WHERE index = 15 LIMIT 1) AS customDim15,
        (SELECT value FROM hits.customdimensions WHERE index = 16 LIMIT 1) AS customDim16,
        (SELECT value FROM hits.customdimensions WHERE index = 17 LIMIT 1) AS customDim17,
        (SELECT value FROM hits.customdimensions WHERE index = 18 LIMIT 1) AS customDim18,
        (SELECT value FROM hits.customdimensions WHERE index = 19 LIMIT 1) AS customDim19,
        (SELECT value FROM hits.customdimensions WHERE index = 20 LIMIT 1) AS customDim20,
        CAST('2021-06-01' AS DATE) AS dt
    FROM `civic-axon-265306.testin.ga_sessions_*` AS sessions,
        UNNEST(hits) AS hits
) hits
LEFT JOIN (
    SELECT
        hits.hitId,
        CASE
            WHEN hits.hitNumber = hits.lastHitNumber THEN NULL
            WHEN hits.hitNumber = hits.lastPageHitNumber THEN hits.lastHitTime - hits.hitTime
            WHEN hits.hitType = 'PAGE' THEN (LEAD(hits.precedingHitTime) OVER (PARTITION BY hits.sessionId, hits.hitType ORDER BY hits.hitNumber ASC) - hits.hitTime)
            ELSE NULL
        END AS timeOnPage
    FROM (
        SELECT
            CONCAT(sessions.fullVisitorId, '_', CAST(visitId AS STRING)) AS sessionId,
            CONCAT(sessions.fullVisitorId, '_', CAST(sessions.visitId AS STRING), '_', hits.hitNumber) AS hitId,
            hits.hitNumber,
            hits.time AS hitTime,
            hits.type AS hitType,
            (SELECT h.hitNumber FROM UNNEST(sessions.hits) h ORDER BY h.hitNumber ASC LIMIT 1) AS firstHitNumber,
            (SELECT h.hitNumber FROM UNNEST(sessions.hits) h ORDER BY h.hitNumber DESC LIMIT 1) AS lastHitNumber,
            (SELECT h.hitNumber FROM UNNEST(sessions.hits) h WHERE h.type = 'PAGE' ORDER BY h.hitNumber ASC LIMIT 1) AS firstPageHitNumber,
            (SELECT h.hitNumber FROM UNNEST(sessions.hits) h WHERE h.type = 'PAGE' ORDER BY h.hitNumber DESC LIMIT 1) AS lastPageHitNumber,
            LAG(hits.time) OVER (PARTITION BY CONCAT(sessions.fullVisitorId, '_', CAST(visitId AS STRING)) ORDER BY hits.hitNumber ASC) AS precedingHitTime,
            LAST_VALUE(hits.time) OVER (PARTITION BY CONCAT(sessions.fullVisitorId, '_', CAST(visitId AS STRING)) ORDER BY hits.hitNumber ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lastHitTime
        FROM `civic-axon-265306.testin.ga_sessions_*` AS sessions,
            UNNEST(hits) AS hits
        WHERE (hits.type != 'EVENT' OR hits.eventInfo.eventCategory NOT IN ('AB Tests'))
        ) hits
) top ON top.hitId = hits.hitId
LEFT JOIN 
    {{ ref('document_services') }}
 documents ON documents.url = REGEXP_REPLACE(hits.urlPath, CONCAT(hits.page.hostname, "/amp"), page.hostname)
WHERE (documents.rn = 1 OR documents.rn IS NULL)

-- civic-axon-265306.testin.