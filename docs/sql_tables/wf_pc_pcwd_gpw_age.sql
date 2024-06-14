--Statement to create the table for main gpw data
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[wf_pc_pcwd_gpw_age] (
	date_extract DATE NOT NULL,
	date_data DATE NOT NULL,
	prac_code CHAR(6) NOT NULL,
	prac_name NVARCHAR(80) NOT NULL,
	source_name NVARCHAR(40) NOT NULL,
    staff_in_post FLOAT NOT NULL
	PRIMARY KEY (date_data, prac_code, source_name)
)