SELECT *
FROM covid_deaths
ORDER BY 2, 3;

SELECT TOP 10 *
FROM covid_vaccinated
ORDER BY 2, 3;

SELECT
	location,
	date,
	total_cases,
	new_cases,
	total_deaths,
	population
FROM covid_deaths
ORDER BY 1, 2;


-- 1. Total Cases vs Total Death (%)

SELECT
	continent,
	location,
	date,
	population,
	total_cases,
	total_deaths,
	ROUND((total_deaths/total_cases)*100, 2) AS death_percentage
FROM covid_deaths
--WHERE location LIKE '%Indo%'
ORDER BY 1, 2, 3;


-- 2. Countries with Highest Infection Rate per Population

SELECT
	location,
	population,
	MAX(total_cases) AS highest_infection,
	ROUND(MAX((total_cases/population))*100, 2) AS percent_infected
FROM covid_deaths
WHERE continent IS NOT NULL 
--AND location LIKE '%Faeroe%'
GROUP BY location, population
ORDER BY 4 DESC;


-- 3. Countries with Highest Death

SELECT
	location,
	population,
	MAX(CAST(total_deaths as INT)) AS total_death,
	ROUND(MAX((CAST(total_deaths as INT)/population))*100, 2) AS percent_death
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY 3 DESC;


-- 4. Continent with Highest Death

SELECT
	location AS continent,
	population,
	MAX(cast(total_deaths as INT)) AS total_death,
	ROUND(MAX((CAST(total_deaths as INT)/population))*100, 2) AS percent_death
FROM covid_deaths
WHERE continent IS NULL
	AND location NOT LIKE '%income%'
--	AND location != 'Upper middle income'
--	AND location != 'High income'
--	AND location != 'Lower middle income'
--	AND location != 'Low income'
	AND location != 'International'
GROUP BY location, population
ORDER BY 3 DESC;


-- 5. Total Cases vs Total Death per Days

SELECT
	date,
	SUM(new_cases) AS total_cases,
	SUM(CAST(new_deaths AS INT)) AS total_deaths,
	ROUND(SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100, 2) AS death_percentage
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2 DESC;


-- 6. Total Cases vs Total Death

SELECT
	SUM(new_cases) AS total_cases,
	SUM(CAST(new_deaths AS INT)) AS total_deaths,
	ROUND(SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100, 2) AS death_percentage
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1, 2 DESC;


-- 7. Total Population vs Total Vaccinations

SELECT
	cd.continent,
	cd.location,
	cd.date,
	cd.population,
	cv.new_vaccinations,
	SUM(CONVERT(BIGINT, cv.new_vaccinations))
		OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) AS cumulative_vaccinated
--	ROUND((SUM(CONVERT(BIGINT, cv.new_vaccinations))
--		OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date)/cd.population)*100, 2) AS percentage_vaccinated
FROM covid_deaths AS cd
JOIN covid_vaccinated AS cv
	ON cd.location = cv.location
	AND cd.date = cv.date
WHERE cd.continent IS NOT NULL
ORDER BY 2, 3;


-- 8. Total Population vs Total Vaccinated USE CTE

WITH pop_vs_vac (continent, location, date, population, new_vaccination, cumulative_vaccinated)
AS
(
	SELECT
		cd.continent,
		cd.location,
		cd.date,
		cd.population,
		cv.new_vaccinations,
		SUM(CONVERT(BIGINT, cv.new_vaccinations))
			OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) AS cumulative_vaccinated
--		ROUND((SUM(CONVERT(BIGINT, cv.new_vaccinations))
--			OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date)/cd.population)*100, 2) AS percentage_vaccinated
	FROM covid_deaths AS cd
	JOIN covid_vaccinated AS cv
		ON cd.location = cv.location
		AND cd.date = cv.date
	WHERE cd.continent IS NOT NULL
)

SELECT *, ROUND((cumulative_vaccinated/population)*100, 2) AS percent_total_vaccinated
FROM pop_vs_vac


-- 9. Total Population vs Total Vaccinated USING TEMPORARY TABLE

DROP TABLE IF EXISTS #percent_population_vaccinated
CREATE TABLE #percent_population_vaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
cumulative_vaccinated numeric
)

INSERT INTO #percent_population_vaccinated
	SELECT
		cd.continent,
		cd.location,
		cd.date,
		cd.population,
		cv.new_vaccinations,
		SUM(CONVERT(BIGINT, cv.new_vaccinations))
			OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) AS cumulative_vaccinated
--		ROUND((SUM(CONVERT(BIGINT, cv.new_vaccinations))
--			OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date)/cd.population)*100, 2) AS percentage_vaccinated
	FROM covid_deaths AS cd
	JOIN covid_vaccinated AS cv
		ON cd.location = cv.location
		AND cd.date = cv.date
	WHERE cd.continent IS NOT NULL

SELECT *, (cumulative_vaccinated/population)*100 AS percent_total_vaccinated
FROM #percent_population_vaccinated


-- 10. Total Population vs People Fully Vaccinated

SELECT
	g.continent,
	g.location,
	g.date,
	g.population,
	MAX(CONVERT(BIGINT, cv.people_fully_vaccinated))
		OVER (PARTITION BY g.location, g.grouper ORDER BY g.location, g.date) AS cumulative_fully_vaccinated,
	ROUND((MAX(CONVERT(BIGINT, cv.people_fully_vaccinated))
		OVER (PARTITION BY g.location, g.grouper ORDER BY g.location, g.date)/g.population)*100, 2) AS cumulative_percentage
FROM 
	(
		SELECT
			cd.continent,
			cd.location,
			cd.date,
			cd.population,
			COUNT(CONVERT(BIGINT, cv.people_fully_vaccinated))
				OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) AS grouper
		FROM covid_deaths AS cd
		JOIN covid_vaccinated AS cv
			ON cd.location = cv.location
			AND cd.date = cv.date
		WHERE cd.continent IS NOT NULL
	) AS g
JOIN covid_vaccinated AS cv
	ON g.location = cv.location
	AND g.date = cv.date
WHERE g.continent IS NOT NULL --AND g.location LIKE '%Arab%'
ORDER BY 2, 3;


-- 10. Positive rate per date

SELECT
	d.continent,
	d.location,
	d.date,
	d.population,
	v.positive_rate
FROM covid_deaths AS d
JOIN covid_vaccinated AS v
ON d.location = v.location
AND d.date = v.date
ORDER BY 2, 3;


-- 11. Test performed each days
 
SELECT
	d.location,
	d.date,
	v.new_tests
FROM covid_deaths AS d
JOIN covid_vaccinated AS v
ON d.location = v.location
AND d.date = v.date
WHERE d.continent IS NOT NULL
ORDER BY 1, 2;


-- 12. Total test per total confirmed case
-- How many test does a country do to find one cases

SELECT
	d.location,
	d.date,
	d.total_cases,
	v.total_tests,
	ROUND((v.total_tests/d.total_cases)*100, 2)
FROM covid_deaths AS d
JOIN covid_vaccinated AS v
ON d.location = v.location
AND d.date = v.date
ORDER BY 1, 2;


-- 13. Hospitailzed patient, ICU

SELECT
	location,
	date,
	icu_patients,
	hosp_patients
FROM covid_deaths
ORDER BY 1, 2;


-- Creating View Example to Store data for Visualization
-- So you can auto-query to BI Tools (Tableau) without the need to save as CSV or TXT

CREATE VIEW percent_population_vaccinated AS
SELECT
	g.continent,
	g.location,
	g.date,
	g.population,
	MAX(CONVERT(BIGINT, cv.people_fully_vaccinated))
		OVER (PARTITION BY g.location, g.grouper ORDER BY g.location, g.date) AS cumulative_fully_vaccinated,
	ROUND((MAX(CONVERT(BIGINT, cv.people_fully_vaccinated))
		OVER (PARTITION BY g.location, g.grouper ORDER BY g.location, g.date)/g.population)*100, 2) AS cumulative_percentage
FROM 
	(
		SELECT
			cd.continent,
			cd.location,
			cd.date,
			cd.population,
			COUNT(CONVERT(BIGINT, cv.people_fully_vaccinated))
				OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) AS grouper
		FROM covid_deaths AS cd
		JOIN covid_vaccinated AS cv
			ON cd.location = cv.location
			AND cd.date = cv.date
		WHERE cd.continent IS NOT NULL
	) AS g
JOIN covid_vaccinated AS cv
	ON g.location = cv.location
	AND g.date = cv.date
WHERE g.continent IS NOT NULL --AND g.location LIKE '%Arab%'

SELECT *
FROM percent_population_vaccinated