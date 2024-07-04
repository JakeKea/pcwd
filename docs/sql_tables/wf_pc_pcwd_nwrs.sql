--Statement to create the table for the NWRS metadata
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[wf_pc_pcwd_nwrs] (
	date_extract DATE NOT NULL,
	date_data DATE NOT NULL,
    scope NVARCHAR(4) NOT NULL,
    scope_id CHAR(6) NOT NULL,
    prac_code CHAR(6),
    prac_name NVARCHAR(80),
	pcn_code CHAR(6) NOT NULL,
	pcn_name NVARCHAR(80) NOT NULL,
	last_logged_in DATE NOT NULL,
	last_modified DATE NOT NULL,
	rag INT NOT NULL
	PRIMARY KEY (date_data, scope_id)
)