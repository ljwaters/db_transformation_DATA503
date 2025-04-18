START TRANSACTION;

-- Company
INSERT INTO jobs.company (display_name)
SELECT DISTINCT job->'company'->>'display_name' AS display_name
FROM jobs.scraped_jobs, jsonb_array_elements(raw_json->'results') AS job
WHERE job->'company' IS NOT NULL
ON CONFLICT (display_name) DO NOTHING;

-- Category
INSERT INTO jobs.category (tag, label)
SELECT DISTINCT
    job->'category'->>'tag' AS tag,
    job->'category'->>'label' AS label
FROM jobs.scraped_jobs, jsonb_array_elements(raw_json->'results') AS job
WHERE job->'category' IS NOT NULL
ON CONFLICT (tag) DO NOTHING;

-- Location
INSERT INTO jobs.location (city, county, state, latitude, longitude)
SELECT DISTINCT
    job->'location'->'area'->>3 AS city,
    job->'location'->'area'->>2 AS county,
    job->'location'->'area'->>1 AS state,
    (job->>'latitude')::DOUBLE PRECISION AS latitude,
    (job->>'longitude')::DOUBLE PRECISION AS longitude
FROM jobs.scraped_jobs,
     jsonb_array_elements(raw_json->'results') AS job
ON CONFLICT (city, county, state) DO NOTHING;

-- Jobs
INSERT INTO jobs.job (
    job_id, adref, title, company_id, category_id, location_id,
    salary_min, salary_max, salary_is_predicted,
    description, redirect_url, created
)
SELECT
    (job->>'id')::BIGINT,
    job->>'adref',
    job->>'title',
    comp.company_id,
    cat.category_id,
    loc.location_id,
    (job->>'salary_min')::NUMERIC,
    (job->>'salary_max')::NUMERIC,
    (job->>'salary_is_predicted')::BOOLEAN,
    job->>'description',
    job->>'redirect_url',
    (job->>'created')::TIMESTAMPTZ
FROM jobs.scraped_jobs,
     jsonb_array_elements(raw_json->'results') AS job
LEFT JOIN jobs.company comp ON comp.display_name = job->'company'->>'display_name'
LEFT JOIN jobs.category cat ON cat.tag = job->'category'->>'tag'
LEFT JOIN jobs.location loc ON loc.city = job->'location'->'area'->>3
                      AND loc.county = job->'location'->'area'->>2
                      AND loc.state = job->'location'->'area'->>1
ON CONFLICT (job_id) DO NOTHING;

DELETE FROM jobs.scraped_jobs;

COMMIT;
