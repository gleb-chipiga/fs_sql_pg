DROP TYPE IF EXISTS public.session_type;

CREATE TYPE public.session_type AS ENUM
    ('found', 'not_found', 'ym');

WITH table_unnest AS (
	SELECT
		DISTINCT ON (v.visit_id, h.date, h.client_id, h.url)
		v.visit_id,
		h.date,
		h.date_time,
		h.client_id,
		h.url,
		h.utm_source,
		h.utm_medium,
		h.utm_campaign,
		h.utm_content,
		h.utm_term,
		1 AS session_count
	FROM visits as v
	JOIN hits as h ON h.client_id = v.client_id AND (h.watch_id = ANY (v.watch_ids))
	ORDER BY v.visit_id, h.date, h.client_id, h.url, h.date_time
),

cost_table AS (
	SELECT
		*,
		-floor(random()* 99999999999999999999 + 1)::numeric AS client_id
	FROM (
		SELECT
			date,
			ad_id,
			campaign_name,
			SUM(cost) AS cost,
			'vk' AS source,
			'paid' AS medium
		FROM "costs_vk_20230205"
		GROUP BY date, ad_id, campaign_name
		UNION
		SELECT
			date,
			ad_id,
			campaign_name,
			SUM(cost) AS cost,
			'yandex_direct' AS source,
			'paid' AS medium
		FROM "costs_yandex_20230205"
		GROUP BY date, ad_id, campaign_name
		UNION
		SELECT
			date,
			banner_id AS ad_id,
			campaign_name,
			SUM(spent) AS cost,
			'MyTarget' AS source,
			'paid' AS medium
		FROM "costs_mytarget_20230205"
		GROUP BY date, ad_id, campaign_name
	) AS ct
),

table_utm_edit as (
    SELECT
        date,
        url,
        client_id,
        visit_id,
        session_count,
		utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        CASE
     	    WHEN utm_source = 'yandex' THEN utm_content
          	ELSE utm_term
        END AS utm_term
    FROM table_unnest
),

table_utm_edit_final AS (
	SELECT
		date,
		utm_source,
		utm_medium,
		utm_campaign,
		visit_id,
		client_id,
		session_count,
		substring(utm_term, '^\d{7,11}$')::integer AS utm_term
	FROM table_utm_edit
),

cost_table_join_sessions AS (
    SELECT
		c.date,
		t.utm_source AS visit_source,
		c.source AS cost_source,
		c.medium,
		c.campaign_name,
		c.ad_id,
		c.cost,
		COUNT(t.visit_id) AS visit_count
    FROM cost_table AS c
    LEFT JOIN table_utm_edit_final AS t ON c.date = t.date AND c.ad_id = t.utm_term
    GROUP BY
		c.date,
		c.ad_id,
		t.utm_source,
		c.cost,
		c.source,
		c.medium,
		c.campaign_name
    ORDER BY date ASC
),

found_or_notfound_source AS (
	SELECT
		'found'::session_type AS session_type,
		date,
		ad_id,
		visit_source,
		cost_source,
		medium,
		campaign_name,
		SUM(cost) AS cost,
		visit_count,
		null as client_id
	FROM cost_table_join_sessions
	WHERE visit_source IS NOT NULL
	GROUP BY
		visit_source,
		cost_source,
		ad_id,
		date,
		medium,
		visit_count,
		campaign_name,
		visit_count
	UNION
	SELECT
		'not_found'::session_type AS session_type,
		cs.date,
		cs.ad_id,
		cs.visit_source,
		cs.cost_source,
		cs.medium,
		cs.campaign_name,
		SUM(cs.cost) AS cost,
		cs.visit_count,
		c.client_id
	FROM cost_table_join_sessions AS cs
	LEFT JOIN cost_table AS c USING(date, ad_id)
	WHERE visit_source IS NULL
	GROUP BY
		source,
		visit_source,
		cost_source,
		cs.ad_id,
		cs.date,
		cs.medium,
		cs.campaign_name,
		visit_count,
		client_id
),

found_or_notfound_source_JOIN_cost_table_join_sessions AS (
	SELECT
		f.session_type,
		f.date,
		f.ad_id,
		f.cost_source,
		f.visit_source,
		f.medium,
		f.campaign_name,
		f.cost,
		round((f.cost / CASE WHEN f.visit_count = 0 THEN 1 ELSE f.visit_count END)::numeric, 2) AS avg_session_cost,
		s.visit_count,
		f.client_id
	FROM found_or_notfound_source AS f
	LEFT JOIN cost_table_join_sessions AS s USING(date, ad_id)
),

pre_final_tab AS (
	SELECT
		f.session_type,
		f.date,
		f.ad_id,
		f.cost_source,
		f.visit_source,
		f.medium,
		f.campaign_name,
		f.cost,
		f.avg_session_cost,
		f.visit_count,
		t.visit_id,
		t.client_id
	FROM found_or_notfound_source_JOIN_cost_table_join_sessions AS f
	LEFT JOIN table_utm_edit_final AS t ON f.date = t.date AND f.ad_id = t.utm_term
	WHERE session_type = 'found'::session_type
	UNION
	SELECT
		DISTINCT ON (date, ad_id)
		session_type,
		date,
		ad_id,
		cost_source,
		visit_source,
		medium,
		campaign_name,
		cost,
		cost as avg_session_cost,
		0 as visit_count,
		-floor(random()* 99999999999999999999 + 1)::numeric AS visit_id,
		client_id
	FROM found_or_notfound_source
	WHERE session_type = 'found'::session_type
	ORDER BY date, ad_id
),

find_double_ad_click_cases AS (
	SELECT
		session_type,
		date,
		ad_id,
		cost_source,
		visit_source,
		medium,
		campaign_name,
		SUM(avg_session_cost) as avg_session_cost,
		visit_count,
		visit_id,
		client_id
	FROM pre_final_tab
	GROUP BY
		session_type,
		date,
		ad_id,
		cost_source,
		visit_source,
		medium,
		campaign_name,
		visit_count,
		visit_id,
		client_id
)

SELECT *
FROM
(
	SELECT
		u.session_type,
		u.date,
		CASE WHEN gt.visit_id IS NOT NULL THEN gt.source_a ELSE source END source,
		CASE WHEN gt.visit_id IS NOT NULL THEN gt.medium_a ELSE medium END medium,
		CASE WHEN gt.visit_id IS NOT NULL THEN campaign_name ELSE null END campaign_name,
		CASE WHEN gt.visit_id IS NOT NULL THEN ad_id ELSE null END ad_id,
		CASE WHEN gt.visit_id IS NOT NULL THEN gt.commission ELSE u.avg_session_cost END avg_session_cost,
		u.session_count,
		u.session_id,
		u.client_id,
		CASE WHEN gt.visit_id IS NOT NULL THEN gt.source_a ELSE source END source,
		CASE WHEN gt.visit_id IS NOT NULL THEN 'adv_map' ELSE null END map,
	FROM
	(
		SELECT
			session_type,
			date,
			ad_id,
			cost_source,
			visit_source,
			medium,
			campaign_name,
			avg_session_cost,
			visit_count,
			visit_id,
			client_id
		FROM find_double_ad_click_cases
		UNION ALL
		SELECT
			'ym'::session_type as session_type,
			date,
			utm_term as ad_id,
			null as cost_source,
			session_count as visit_source,
			utm_mediaum as medium,
			'' as campaign_name, -- traffic_name_GA4 as campaign_name,
			0 as avg_session_cost,
			visit_count,
			visit_id,
			user_pseudo_id as client_id
		FROM table_utm_edit
		-- WHERE is_paid_sessions = 'unpaid_sessions'
	) u
	LEFT JOIN `costs_advcake` ON u.visit_id = gt.session_id
)
