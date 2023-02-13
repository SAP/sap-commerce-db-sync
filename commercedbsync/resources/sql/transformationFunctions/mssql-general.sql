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

CREATE OR ALTER FUNCTION mask_custom(@Prefix int, @Mask varchar(max), @Suffix int, @Original varchar(MAX))
RETURNS VARCHAR(max)
AS
BEGIN

     RETURN SUBSTRING(@Original,1,@Prefix) + 
     @Mask +
     SUBSTRING(@Original,LEN(@Original) - @Suffix + 1, LEN(@Original))
END;

CREATE OR ALTER FUNCTION mask_email(@String varchar(MAX))
RETURNS VARCHAR(max)
AS
BEGIN

     RETURN LEFT(@String, 3) + '*****@' 
        + REVERSE(LEFT(RIGHT(REVERSE(@String) , CHARINDEX('@', @String) +2), 2))
        + '******'
        + RIGHT(@String, 4)
END;