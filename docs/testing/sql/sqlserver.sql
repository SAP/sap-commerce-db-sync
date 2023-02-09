CREATE TABLE regionss (
	region_id INT IDENTITY(1,1) PRIMARY KEY,
	region_name VARCHAR (25) DEFAULT NULL
);

CREATE TABLE countriess (
	country_id CHAR (2) PRIMARY KEY,
	country_name VARCHAR (40) DEFAULT NULL,
	region_id INT NOT NULL,
	FOREIGN KEY (region_id) REFERENCES regionss (region_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE locationss (
	location_id INT IDENTITY(1,1) PRIMARY KEY,
	street_address VARCHAR (40) DEFAULT NULL,
	postal_code VARCHAR (12) DEFAULT NULL,
	city VARCHAR (30) NOT NULL,
	state_province VARCHAR (25) DEFAULT NULL,
	country_id CHAR (2) NOT NULL,
	FOREIGN KEY (country_id) REFERENCES countriess (country_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE jobss (
	job_id INT IDENTITY(1,1) PRIMARY KEY,
	job_title VARCHAR (35) NOT NULL,
	min_salary DECIMAL (8, 2) DEFAULT NULL,
	max_salary DECIMAL (8, 2) DEFAULT NULL
);

CREATE TABLE departmentss (
	department_id INT IDENTITY(1,1) PRIMARY KEY,
	department_name VARCHAR (30) NOT NULL,
	location_id INT DEFAULT NULL,
	FOREIGN KEY (location_id) REFERENCES locationss (location_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE employeess (
	employee_id INT IDENTITY(1,1) PRIMARY KEY,
	first_name VARCHAR (20) DEFAULT NULL,
	last_name VARCHAR (25) NOT NULL,
	email VARCHAR (100) NOT NULL,
	phone_number VARCHAR (20) DEFAULT NULL,
	pass VARCHAR (20) NOT NULL,
	hire_date DATE NOT NULL,
	job_id INT NOT NULL,
	salary DECIMAL (8, 2) NOT NULL,
	manager_id INT DEFAULT NULL,
	department_id INT DEFAULT NULL,
	FOREIGN KEY (job_id) REFERENCES jobss (job_id) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (department_id) REFERENCES departmentss (department_id) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (manager_id) REFERENCES employeess (employee_id)
);

CREATE TABLE dependents (
	dependent_id INT IDENTITY(1,1) PRIMARY KEY,
	first_name VARCHAR (50) NOT NULL,
	last_name VARCHAR (50) NOT NULL,
	relationship VARCHAR (25) NOT NULL,
	employee_id INT NOT NULL,
	FOREIGN KEY (employee_id) REFERENCES employeess (employee_id) ON DELETE CASCADE ON UPDATE CASCADE
);

GO

CREATE OR ALTER VIEW employees_view
AS 
SELECT 
	employee_id,
	first_name,
	last_name,
	email,
	phone_number,
	'xxxx' as pass,
	hire_date,
	job_id,
	salary,
	manager_id,
	department_id
FROM employeess;

-- CREATE OR ALTER VIEW users_view
-- AS 
-- SELECT hjmpTS,createdTS,modifiedTS,TypePkString,OwnerPkString,PK,sealed,p_description,p_name,p_uid,p_profilepicture,p_backofficelogindisabled,p_defaultpaymentaddress,p_defaultshipmentaddress,p_passwordencoding,'xxx' as passwd,'none' as p_passwordanswer,'empty' as p_passwordquestion,p_sessionlanguage,p_sessioncurrency,p_logindisabled,p_lastlogin,p_hmclogindisabled,p_retentionstate,p_userprofile,p_deactivationdate,p_randomtoken,p_europe1pricefactory_udg,p_europe1pricefactory_upg,p_europe1pricefactory_utg,aCLTS,propTS,p_customerid,p_previewcatalogversions FROM users


-- CREATE OR ALTER VIEW v_users
-- AS 
-- SELECT hjmpTS,createdTS,modifiedTS,TypePkString,OwnerPkString,PK,sealed,p_description,p_name,p_uid,p_profilepicture,p_backofficelogindisabled,p_defaultpaymentaddress,p_defaultshipmentaddress,p_passwordencoding,'aaa' as passwd,'bbbb' as p_passwordanswer,'CCCC' as p_passwordquestion,p_sessionlanguage,p_sessioncurrency,p_logindisabled,p_lastlogin,p_hmclogindisabled,p_retentionstate,p_userprofile,p_deactivationdate,p_randomtoken,p_europe1pricefactory_udg,p_europe1pricefactory_upg,p_europe1pricefactory_utg,aCLTS,propTS,p_customerid,p_previewcatalogversions FROM users