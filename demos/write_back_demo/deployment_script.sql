create or replace TABLE EMPLOYEES (
	EMPLOYEE_ID VARCHAR(36) NOT NULL DEFAULT UUID_STRING(),
	EMPLOYEE_UID VARCHAR(36) NOT NULL DEFAULT UUID_STRING(),
	VERSION_NUMBER NUMBER(38,0) NOT NULL DEFAULT 1,
	IS_CURRENT BOOLEAN NOT NULL DEFAULT TRUE,
	FIRST_NAME VARCHAR(100) NOT NULL,
	LAST_NAME VARCHAR(100) NOT NULL,
	EMAIL VARCHAR(254) NOT NULL,
	DEPARTMENT VARCHAR(100),
	FUNCTION VARCHAR(100),
	TITLE VARCHAR(150),
	LOCATION VARCHAR(100),
	MANAGER_EMAIL VARCHAR(254),
	HIRE_DATE DATE,
	ACTIVE BOOLEAN DEFAULT TRUE,
	INSERT_USER VARCHAR(50) DEFAULT CURRENT_USER(),
	INSERT_DATE_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	UPDATE_USER VARCHAR(50),
	UPDATE_DATE_TIME TIMESTAMP_NTZ(9),
	DELETE_USER VARCHAR(50),
	DELETE_DATE_TIME TIMESTAMP_NTZ(9),
	SKILLS VARCHAR(500),
	RESUME_URL VARCHAR(500),
	constraint PK_EMPLOYEES primary key (EMPLOYEE_ID)
);

INSERT INTO HRDEMO.EMPLOYEES
  (VERSION_NUMBER, IS_CURRENT, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, FUNCTION, TITLE, LOCATION, MANAGER_EMAIL, HIRE_DATE, ACTIVE, SKILLS, RESUME_URL)
WITH
  gen AS (
    SELECT SEQ4() AS i
    FROM TABLE(GENERATOR(ROWCOUNT=>100))
  ),
  lists AS (
    SELECT
      ARRAY_CONSTRUCT('Alice','Brian','Chitra','Diego','Emily','Farah','Gabriel','Hana','Ivan','Julia','Kareem','Lara','Mateo','Nisha','Omar','Priya','Quentin','Rosa','Samir','Tina','Uma','Victor','Wendy','Xavier','Yara','Zane','Noah','Ava','Mia','Liam','Ethan','Olivia') AS FIRSTS,
      ARRAY_CONSTRUCT('Engineering','Sales','HR','Finance','Marketing','Operations','IT','Support','Product','Legal') AS DEPTS,
      ARRAY_CONSTRUCT('Data','Platform','Security','Field','PeopleOps','FP&A','Growth','Logistics','CustomerCare','Management') AS FUNCS,
      ARRAY_CONSTRUCT('Data Engineer','Data Scientist','SRE','DevOps Engineer','Security Engineer','Product Manager','Associate PM','Finance Analyst','Recruiter','Account Executive','Sales Engineer','Operations Manager','Support Specialist','Software Engineer') AS TITLES,
      ARRAY_CONSTRUCT('San Francisco','Austin','London','Toronto','Remote','New York','Chicago','Seattle','Berlin','Madrid','Singapore','Dubai') AS LOCS,
      ARRAY_CONSTRUCT('cto@example.com','eng.dir@example.com','hr.head@example.com','sales.dir@example.com','cfo@example.com','cio@example.com','ops.head@example.com','product.dir@example.com','support.dir@example.com') AS MGRS,
      ARRAY_CONSTRUCT('SQL','Python','Snowflake','Streamlit','Data Modeling','Airflow','DBT','Spark','AWS','Kubernetes','CI/CD','Looker','Tableau','Power BI','Java','Scala','Go','Node.js','Docker','Linux','Git','Terraform','Databricks','Azure','GCP') AS SKILLS,
      /* Last name synthesis parts (A + B) */
      ARRAY_CONSTRUCT('Ash','Beck','Black','Brook','Carl','Clark','Cole','Cran','Dun','East','Ell','Fair','Fitz','Flem','Frost','Glen','Gold','Gray','Green','Hart','Hay','Hill','Holt','King','Lake','Mar','North','Oaks','Park','Ray','Rich','South') AS LAST_A,
      ARRAY_CONSTRUCT('ford','man','son','ley','ston','wood','well','field','brook','berg','ton','smith','worth','ridge','stone','wall','more','dale','burn','win','shaw','house','port','worth','water','ham','banks','stead','field','hurst','mont','croft') AS LAST_B
  )
SELECT
  1 AS VERSION_NUMBER,
  TRUE AS IS_CURRENT,
  (lists.FIRSTS[MOD(gen.i, ARRAY_SIZE(lists.FIRSTS))])::string AS FIRST_NAME,
  /* Synthesize varied last names */
  (lists.LAST_A[MOD(gen.i, ARRAY_SIZE(lists.LAST_A))])::string ||
  (lists.LAST_B[MOD(gen.i + ABS(HASH('ln', gen.i))::int, ARRAY_SIZE(lists.LAST_B))])::string AS LAST_NAME,
  LOWER((lists.FIRSTS[MOD(gen.i, ARRAY_SIZE(lists.FIRSTS))])::string)
    ||'.'||LOWER(
      (lists.LAST_A[MOD(gen.i, ARRAY_SIZE(lists.LAST_A))])::string ||
      (lists.LAST_B[MOD(gen.i + ABS(HASH('ln', gen.i))::int, ARRAY_SIZE(lists.LAST_B))])::string
    )||'.'||TO_VARCHAR(gen.i+1)||'@example.com' AS EMAIL,
  (lists.DEPTS[MOD(gen.i, ARRAY_SIZE(lists.DEPTS))])::string AS DEPARTMENT,
  (lists.FUNCS[MOD(gen.i, ARRAY_SIZE(lists.FUNCS))])::string AS FUNCTION,
  (lists.TITLES[MOD(gen.i, ARRAY_SIZE(lists.TITLES))])::string AS TITLE,
  (lists.LOCS[MOD(gen.i, ARRAY_SIZE(lists.LOCS))])::string AS LOCATION,
  (lists.MGRS[MOD(gen.i, ARRAY_SIZE(lists.MGRS))])::string AS MANAGER_EMAIL,
  DATEADD('day', MOD(gen.i, 2500), TO_DATE('2018-01-01')) AS HIRE_DATE,
  IFF(MOD(gen.i, 10)=0, FALSE, TRUE) AS ACTIVE,
  /* Non-uniform skill mix (≥5 per row) */
  (
    (lists.SKILLS[MOD(gen.i + ABS(HASH('a', gen.i))::int, ARRAY_SIZE(lists.SKILLS))])::string || ', ' ||
    (lists.SKILLS[MOD(gen.i + ABS(HASH('b', gen.i))::int + 1, ARRAY_SIZE(lists.SKILLS))])::string || ', ' ||
    (lists.SKILLS[MOD(gen.i + ABS(HASH('c', gen.i))::int + 2, ARRAY_SIZE(lists.SKILLS))])::string || ', ' ||
    (lists.SKILLS[MOD(gen.i + ABS(HASH('d', gen.i))::int + 3, ARRAY_SIZE(lists.SKILLS))])::string || ', ' ||
    (lists.SKILLS[MOD(gen.i + ABS(HASH('e', gen.i))::int + 4, ARRAY_SIZE(lists.SKILLS))])::string
  ) AS SKILLS,
  'https://www.snowflake.com/en/' AS RESUME_URL
FROM gen
CROSS JOIN lists;
