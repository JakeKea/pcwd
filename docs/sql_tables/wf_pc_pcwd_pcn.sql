--Statement to create the table for pcn data
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[wf_pc_pcwd_pcn] (
	date_extract DATE NOT NULL,
	date_data DATE NOT NULL,
	pcn_code CHAR(6) NOT NULL,
	pcn_name NVARCHAR(80) NOT NULL,
	staff_group NVARCHAR(40) NOT NULL,
	staff_role NVARCHAR(100) NOT NULL,
	--staff_role_detailed NVARCHAR(120) NOT NULL,
    fte FLOAT NOT NULL
	PRIMARY KEY (date_data, pcn_code, staff_role)
)