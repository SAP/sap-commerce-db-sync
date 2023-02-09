CREATE OR ALTER FUNCTION mask_custom(@Prefix int, @Mask varchar(max), @Suffix int, @Original varchar(MAX))
RETURNS VARCHAR(max)
AS
BEGIN

     RETURN SUBSTRING(@Original,1,@Prefix) + 
     @Mask +
     SUBSTRING(@Original,LEN(@Original) - @Suffix + 1, LEN(@Original))
END

GO

declare @temp table
(
	col1 varchar(50)
)

insert into @temp
values ('384844033434'), ('743423547878'), ('111224678885');

select dbo.mask_custom(2, 'XXXX', 2, col1) col1 from @temp