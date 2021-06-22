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
    {% for i in range(20) %}
        hits.customDim{{ i+1 }},
    {% endfor %}
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
        {% for i in range(20) %}
            {{ index_to_custom_dimensions(i+1) }}
        {% endfor %}
        
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