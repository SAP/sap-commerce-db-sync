CREATE OR ALTER FUNCTION mask_email(@String varchar(MAX))
RETURNS VARCHAR(max)
AS
BEGIN

     RETURN LEFT(@String, 3) + '*****@' 
        + REVERSE(LEFT(RIGHT(REVERSE(@String) , CHARINDEX('@', @String) +2), 2))
        + '******'
        + RIGHT(@String, 4)
END

GO

declare @temp table
(
	col1 varchar(50)
)

insert into @temp
values ('john.doe@gmail.com'), ('elvis@bbc.co.uk'), ('boris@gov.uk');

select dbo.mask_email(col1) col1 from @temp;