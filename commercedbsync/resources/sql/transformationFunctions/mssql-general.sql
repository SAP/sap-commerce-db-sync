CREATE OR ALTER FUNCTION mask_str (@originialvalue nvarchar(255), @maskedvalue nvarchar(255))
RETURNS nvarchar(255)
AS
BEGIN
IF @originialvalue IS NULL
	RETURN NULL

RETURN @maskedvalue
END;


CREATE OR ALTER FUNCTION mask_unique_str (@pk bigint, @suffix nvarchar(255))
RETURNS nvarchar(255)
AS
BEGIN   
RETURN CONCAT(@pk, @suffix)
END;

CREATE OR ALTER FUNCTION mask_password (@uid nvarchar(255))
RETURNS nvarchar(255)
AS
BEGIN
IF @uid = 'admin'
	RETURN 'nimda'

RETURN NULL
END;

CREATE OR ALTER FUNCTION mask_passwordencoding (@uid nvarchar(255))
RETURNS nvarchar(255)
AS
BEGIN
IF @uid = 'admin'
	RETURN 'plain'

RETURN '*'
END;