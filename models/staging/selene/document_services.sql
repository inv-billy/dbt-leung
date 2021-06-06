SELECT url, doc_id, ROW_NUMBER() OVER (PARTITION BY url ORDER BY updated_timestamp DESC) AS rn
    FROM `civic-axon-265306.testin.document_services_*`
    WHERE template_type NOT IN ('REDIRECT')