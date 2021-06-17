<template>
  <pre>
USE [{{settings.databases.staging.name}}]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create PROCEDURE [migration].[migrate_crh]
AS
BEGIN

  begin try

    declare @trancount int;
    set @trancount = @@TRANCOUNT;

    exec [migration].disableAllForeignKeys;
    print CONVERT(varchar, SYSDATETIME(), 121) + ' Deleting data '

    DELETE FROM [CRT].updatedate WHERE register = 'CRH'
    DELETE FROM [CRTCRH].[contractregistrationservice].[migration].request_audit_info_CRH_tmp
    --DELETE FROM [contractregistrationservice].[migration].request_audit_info_tmp

    print CONVERT(varchar, SYSDATETIME(), 121) + ' Start inserting '
    --RAISERROR(N'', 0, 1) WITH NOWAIT


    insert into [CRT].address_abroad (country,
                                      municipality,
                                      house_number,
                                      street,
                                      type_map,
                                      map_id)
    select country,
           municipality,
           house_number,
           street,
           type_map,
           map_id
    from [CRTCRH].[contractregistrationservice].[migration].penholder_crh_view ph
	left outer join crt.signatory s
	on ph.map_id = s.penholder_map_id
	where s.penholder_map_id is null
    --from [contractregistrationservice].[migration].penholder_view


    INSERT INTO [CRT].signatory(organization_id,
                                organization_name,
                                notary_id,
                                notary_firstname,
                                notary_lastname,
                                nrn,
                                signatory_type,
                                address_abroad_id,
                                notary_abroad_name,
                                study_abroad_name,
                                penholder_map_id)
    select t1.organization_id,
           t1.organization_name,
           t1.notary_id,
           t1.notary_firstname,
           t1.notary_lastname,
           t1.nrn,
           t1.signatory_type,
           t2.address_abroad_id,
           t1.notary_abroad_name,
           t1.study_abroad_name,
           t1.penholder_map_id
    from [CRTCRH].[contractregistrationservice].[migration].signatory_crh_view t1
	left outer join crt.address_abroad t2
	on t1.penholder_map_id = t2.map_id
	left outer join crt.signatory s
	on t1.penholder_map_id = s.penholder_map_id
	where s.penholder_map_id is null
    --from [contractregistrationservice].[migration].signatory_view



    INSERT INTO [CRTCRH].[contractregistrationservice].[migration].[request_audit_info_CRH_tmp]
      --INSERT INTO [contractregistrationservice].[migration].[request_audit_info_tmp]
    ([creatorname]
    ,marriagecontractregistration_id
    ,[organizationname]
    ,[studyreference]
    ,[creatorpersonid])
    SELECT
      creatorname
         ,marriagecontractregistration_id
         ,organizationname
         ,studyreference
         ,creatorpersonid
    FROM [CRTCRH].[contractregistrationservice].[migration].request_audit_info_crh_view
    --FROM [contractregistrationservice].[migration].request_audit_info_view

    DECLARE @partition int, @startRegId bigint, @endRegId bigint
    DECLARE curPartitions CURSOR
      FOR
      SELECT partitionId, startRegId, endRegId FROM [CRTCRH].[contractregistrationservice].[migration].deed_partitions_crh_view
                                                    --SELECT partitionId, startRegId, endRegId FROM [contractregistrationservice].[migration].deed_partitions_view
      order by startRegId desc

    OPEN curPartitions
    FETCH NEXT FROM curPartitions INTO @partition, @startRegId, @endRegId
    WHILE @@FETCH_STATUS = 0
    BEGIN

      print CONVERT(varchar, SYSDATETIME(), 121) + ' Starting id ' + CAST(@startRegId as VARCHAR(10)) + ' to ' + CAST(@endRegId as VARCHAR(10))
      --RAISERROR(N'', 0, 1) WITH NOWAIT

      begin tran txPartition

        print CONVERT(varchar, SYSDATETIME(), 121) + ' inserting into inscription_requester '
        --RAISERROR(N'', 0, 1) WITH NOWAIT

        INSERT INTO [CRT].inscription_requester(requester_type,
                                                organization_id,
                                                organization_name,
                                                notary_id,
                                                notary_firstname,
                                                notary_lastname,
                                                address_abroad_id,
                                                notary_abroad_name,
                                                study_abroad_name,
                                                requester_map_id,
                                                registration_map_id)
        SELECT irv.requester_type,
               irv.organization_id,
               irv.organization_name,
               irv.notary_id,
               irv.notary_firstname,
               irv.notary_lastname,
               aa.address_abroad_id,
               irv.notary_abroad_name,
               irv.study_abroad_name,
               min(irv.requester_map_id),
               irv.registration_map_id
        FROM  [CRTCRH].[contractregistrationservice].[migration].inscription_requester_crh_view irv
                --FROM  [contractregistrationservice].[migration].inscription_requester_view irv
                LEFT JOIN [CRT].[address_abroad] aa on aa.map_id = irv.address_abroad_id
                LEFT JOIN [CRT].[inscription_requester] ir2 on ir2.registration_map_id = irv.registration_map_id
        WHERE irv.registration_map_id between @startRegId and @endRegId
		and
          ir2.registration_map_id is null
        GROUP BY
          irv.requester_type,
          irv.organization_id,
          irv.organization_name,
          irv.notary_id,
          irv.notary_firstname,
          irv.notary_lastname,
          aa.address_abroad_id,
          irv.notary_abroad_name,
          irv.study_abroad_name,
          irv.registration_map_id

        print CONVERT(varchar, SYSDATETIME(), 121) + ' inserting into paper_deed '
        --RAISERROR(N'', 0, 1) WITH NOWAIT


        INSERT INTO [CRT].paper_deed
        (status_id,
         deed_date,
         dossier_reference,
         repertorium_number,
         request_date,
         signatory_id,
         created_on,
         updated_on,
         updated_by_user_name,
         created_by_organization_name,
         created_by_organization_id,
         created_by_user_name,
         created_by_user_id,
         requester_id,
         updated_by_organization_name,
         updated_by_organization_id,
         updated_by_user_id,
         registration_map,
         marriage_link_map)
        select t1.status_id,
               t1.deed_date,
               t1.dossier_reference,
               t1.repertorium_number,
               t1.request_date,
               ( SELECT signatory_id
           FROM   [CRT].signatory sig
           WHERE  sig.penholder_map_id = t1.Penholder_Id) AS signatory_id,
               t1.created_on,
               t1.updated_on,
               t1.updated_by_user_name,
               t1.created_by_organization_name,
               t1.created_by_organization_id,
               t1.created_by_user_name,
               t1.created_by_user_id,
			   (
               SELECT ir.requester_id
               FROM   [CRT].inscription_requester ir
               WHERE  ir.registration_map_id = t1.Registration_Id)
                as requester_id,
               t1.updated_by_organization_name,
               t1.updated_by_organization_id,
               t1.updated_by_user_id,
               t1.registration_map,
               t1.marriage_link_map
        from [CRTCRH].[contractregistrationservice].[migration].paper_deed_crh_view t1
		left outer join crt.paper_deed pd on t1.registration_id = pd.marriage_link_map
             --from [contractregistrationservice].[migration].paper_deed_view
        where t1.Registration_Id between @startRegId and @endRegId
		and pd.marriage_link_map is null 




        print CONVERT(varchar, SYSDATETIME(), 121) + ' inserting into juridical_deed ';
        --RAISERROR(N'', 0, 1) WITH NOWAIT


        WITH tmp_jdgv AS (
          SELECT [jd].[juridical_deed_number]
               , [jd].[status_id]
               , [jd].[registration_type_id]
               , [jd].[doc_contents_id]
               , [jd].[to_invoice]
               , [jd].[created_on]
               , [jd].[updated_on]
               , [jd].[updated_by_user_name]
               , [jd].[created_by_organization_name]
               , [jd].[created_by_organization_id]
               , [jd].[created_by_user_name]
               , [jd].[created_by_user_id]
               , [jd].[updated_by_organization_name]
               , [jd].[updated_by_organization_id]
               , [jd].[updated_by_user_id]
               , [jd].[operation_type_id]
               , [jd].[description]
               , [jd].[keeps_own_document]
               , [jd].[mapid]
               , [jd].[register]
               , jd.[Registration_Id]
               , jd.[MarriageContractRegistration_Id]
			   , jd.previous_doc_Contents_id
			   , jd.to_publish
			   , jd.publish_language
               FROM  crtcrh.[contractregistrationservice].[migration].[juridical_deed_global_crh_view] jd
          WHERE jd.Registration_Id between @startRegId and @endRegId
        )


        INSERT INTO [CRT].[juridical_deed] (juridical_deed_number
                                           , paper_deed_id
                                           , status_id
                                           , registration_type_id
                                           , doc_contents_id
                                           , to_invoice
                                           , created_on
                                           , updated_on
                                           , updated_by_user_name
                                           , created_by_organization_name
                                           , created_by_organization_id
                                           , created_by_user_name
                                           , created_by_user_id
                                           , updated_by_organization_name
                                           , updated_by_organization_id
                                           , updated_by_user_id
                                           , operation_type_id
                                           , description
                                           , keeps_own_document
                                           , mapid
                                           , register
										   , previous_doc_contents_id
										   , to_publish
										   , publish_language)
        SELECT  [jd].[juridical_deed_number]
             ,[pd1].[paper_deed_id] AS paper_deed_id
             , [jd].[status_id]
             , [jd].[registration_type_id]
             , [jd].[doc_contents_id]
             , [jd].[to_invoice]
             , [jd].[created_on]
             , [jd].[updated_on]
             , [jd].[updated_by_user_name]
             , [jd].[created_by_organization_name]
             , [jd].[created_by_organization_id]
             , [jd].[created_by_user_name]
             , [jd].[created_by_user_id]
             , [jd].[updated_by_organization_name]
             , [jd].[updated_by_organization_id]
             , [jd].[updated_by_user_id]
             , [jd].[operation_type_id]
             , [jd].[description]
             , [jd].[keeps_own_document]
             , [jd].[mapid]
             , [jd].[register]
			 , jd.previous_doc_contents_id
			 , jd.to_publish
			 , jd.publish_language
        FROM  tmp_jdgv jd
                LEFT JOIN [CRT].[paper_deed] pd1 ON [pd1].marriage_link_map  = [jd].[Registration_Id]
               


        print CONVERT(varchar, SYSDATETIME(), 121) + ' inserting into person '
        --RAISERROR(N'', 0, 1) WITH NOWAIT

        INSERT INTO [CRT].person
        (person_type,
         nrn,
         first_name,
         last_name,
         birth_date,
         birth_country_code,
         birth_country_description,
         birth_municipality_nis_code,
         birth_municipality_name,
         juridical_deed_id,
         person_role_id,
         NAME,
         enterprise_number,
         juridical_form,
         party_id_map)
        SELECT [person_type]
             , [nrn]
             , [first_name]
             , [last_name]
             , [birth_date]
             , [birth_country_code]
             , [birth_country_description]
             , [birth_municipality_nis_code]
             , [birth_municipality_name]
             , jd.juridical_deed_id
             , [person_role_id]
             , [name]
             , [enterprise_number]
             , [juridical_form]
             , [party_id_map]
        FROM  [CRTCRH].[contractregistrationservice].[migration].person_crh_view
                --FROM  [contractregistrationservice].[migration].person_view
                join [CRT].juridical_deed jd on jd.mapid = registration_id
        where Registration_Id between @startRegId and @endRegId
        


        print CONVERT(varchar, SYSDATETIME(), 121) + ' inserting into address '

        INSERT INTO [CRT].[address]
        ([person_id]
        ,[country_code]
        ,[country_description]
        ,[municipality_nis_code]
        ,[municipality_name]
        ,[house_number]
        ,[postal_box]
        ,[postal_code]
        ,[street_name]
        ,[type]
        ,[party_id_map])
        SELECT distinct t2.person_id,
               t1.country_code,
               t1.country_name as country_description,
               t1.municipality_nis_code,
               t1.municipality_name,
               t1.house_number,
               t1.postal_box,
               t1.postal_code,
               t1.Street_Name,
               t1.type,
               t1.party_id_map
        FROM CRTCRH.[contractregistrationservice].migration.address_crh_view t1
               inner join crt.person t2 on t1.party_id_map = t2.party_id_map
               inner join crt.juridical_deed t3 on t2.juridical_deed_id = t3.juridical_deed_id
        where  t3.mapid between @startRegId and @endRegId
       



        INSERT INTO [CRT].[address_line]
        ([lines]
        ,[address_id]
        ,[party_id_map])
        SELECT t1.lines, t2.address_id, t1.involvedparty_id
        from crtcrh.contractregistrationservice.migration.address_line_crh_view t1
               inner join crt.address t2 on t1.InvolvedParty_Id = t2.party_id_map
               inner join crt.person t3 on t2.party_id_map = t3.party_id_map
               inner join crt.juridical_deed t4 on t3.juridical_deed_id = t4.juridical_deed_id
        where t4.mapid between @startRegId and @endRegId

      commit tran txPartition

      FETCH NEXT FROM curPartitions INTO @partition, @startRegId, @endRegId

    END

    CLOSE curPartitions
    DEALLOCATE curPartitions;

    WITH tmp_jdgv AS (
      SELECT [jd].[juridical_deed_number]
           , [jd].[status_id]
           , [jd].[registration_type_id]
           , [jd].[doc_contents_id]
           , [jd].[to_invoice]
           , [jd].[created_on]
           , [jd].[updated_on]
           , [jd].[updated_by_user_name]
           , [jd].[created_by_organization_name]
           , [jd].[created_by_organization_id]
           , [jd].[created_by_user_name]
           , [jd].[created_by_user_id]
           , [jd].[updated_by_organization_name]
           , [jd].[updated_by_organization_id]
           , [jd].[updated_by_user_id]
           , [jd].[operation_type_id]
           , [jd].[description]
           , [jd].[keeps_own_document]
           , [jd].[mapid]
           , [jd].[register]
           , jd.[Registration_Id]
           , jd.[MarriageContractRegistration_Id]
      FROM [CRTCRH].[contractregistrationservice].[migration].[juridical_deed_global_crh_view] jd
      WHERE jd.Registration_Id in (
        select Registration_Id
        from crtcrh.contractregistrationservice.crs.marriagecontractregistration
        where number not in (
          select juridical_deed_number collate Latin1_General_CI_AS_KS_WS
          from UnifiedRegistersstaging.crt.juridical_deed)
          and active = 1)
    )
    INSERT INTO [CRT].[juridical_deed] (juridical_deed_number
                                       , paper_deed_id
                                       , status_id
                                       , registration_type_id
                                       , doc_contents_id
                                       , to_invoice
                                       , created_on
                                       , updated_on
                                       , updated_by_user_name
                                       , created_by_organization_name
                                       , created_by_organization_id
                                       , created_by_user_name
                                       , created_by_user_id
                                       , updated_by_organization_name
                                       , updated_by_organization_id
                                       , updated_by_user_id
                                       , operation_type_id
                                       , description
                                       , keeps_own_document
                                       , mapid
                                       , register)
    SELECT  [jd].[juridical_deed_number]
         , ISNULL([pd1].[paper_deed_id], [pd2].[paper_deed_id]) AS paper_deed_id
         , [jd].[status_id]
         , [jd].[registration_type_id]
         , [jd].[doc_contents_id]
         , [jd].[to_invoice]
         , [jd].[created_on]
         , [jd].[updated_on]
         , [jd].[updated_by_user_name]
         , [jd].[created_by_organization_name]
         , [jd].[created_by_organization_id]
         , [jd].[created_by_user_name]
         , [jd].[created_by_user_id]
         , [jd].[updated_by_organization_name]
         , [jd].[updated_by_organization_id]
         , [jd].[updated_by_user_id]
         , [jd].[operation_type_id]
         , [jd].[description]
         , [jd].[keeps_own_document]
         , [jd].[mapid]
         , [jd].[register]
    FROM  [tmp_jdgv]                  jd
            LEFT JOIN [CRT].[paper_deed] pd1 ON [pd1].[registration_map]  = [jd].[Registration_Id]
            LEFT JOIN [CRT].[paper_deed] pd2 ON [pd2].[registration_map] IS NULL
      AND [pd2].[marriage_link_map] = jd.[MarriageContractRegistration_Id]
    WHERE  ISNULL([pd1].[paper_deed_id], [pd2].[paper_deed_id]) IS NOT NULL


     INSERT INTO [CRT].person
        (person_type,
         nrn,
         first_name,
         last_name,
         birth_date,
         birth_country_code,
         birth_country_description,
         birth_municipality_nis_code,
         birth_municipality_name,
         juridical_deed_id,
         person_role_id,
         NAME,
         enterprise_number,
         juridical_form,
         party_id_map)
     select t1.[person_type]
             , t1.[nrn]
             , t1.[first_name]
             , t1.[last_name]
             , t1.[birth_date]
             , t1.[birth_country_code]
             , t1.[birth_country_description]
             , t1.[birth_municipality_nis_code]
             , t1.[birth_municipality_name]
             , t2.juridical_deed_id
             , t1.[person_role_id]
             , t1.[name]
             , t1.[enterprise_number]
             , t1.[juridical_form]
             , t1.[party_id_map] from crtcrh.contractregistrationservice.migration.person_crh_view t1
     inner join crt.juridical_deed t2 on t1.registration_id = t2.mapid
     left outer join crt.person t3 on t3.juridical_deed_id = t2.juridical_deed_id
     where t3.person_id is null




    --INSERT INTO [CRT].consultation_requester
    --(organization_id,
    -- organization_name,
    -- notary_id,
    -- first_name,
    -- last_name,
    -- nrn,
    -- requester_type,
    -- additional_info,
    -- requester_map_id)

    --SELECT [organization_id]
    --     , [organization_name]
    --     , [notary_id]
    --     , [notary_firstname]
    --     , [notary_lastname]
    --     , [nrn]
    --     , [requestertype]
    --     , [reasondescription]
    --     , [requester_map_id]
    --FROM  [CRTCRH].[contractregistrationservice].[migration].[consultation_requester_view]
    --FROM  [contractregistrationservice].[migration].[consultation_requester_view]

    --INSERT INTO [CRT].justification
    --(reason,
    -- consultation_map,
    -- justification_map)
    --SELECT NULL               AS reason,
    --       abstractrequest_id AS consultation_map,
    --       NULL               AS justification_map
    --FROM [CRTCRH].[contractregistrationservice].[CRS].[searchrequest]
    --     --FROM [contractregistrationservice].[CRS].[searchrequest]
    --WHERE proofofdeceasereference IS NOT NULL

    --INSERT INTO [CRT].file_refs
    --(file_name,
    -- type,
    -- reference,
    -- justification_id,
    -- justification_map)
    --SELECT NULL               AS [file_name],
    --       'PDF'              AS type,
    --       proofofdeceasereference,
    --       (SELECT justification_id
    --        FROM [CRT].justification
    --        WHERE consultation_map = abstractrequest_id),
    --       abstractrequest_id AS justification_map
    --FROM [CRTCRH].[contractregistrationservice].[CRS].[searchrequest]
    --     --FROM [contractregistrationservice].[CRS].[searchrequest]
    --WHERE proofofdeceasereference IS NOT NULL



    /*

      INSERT INTO [CRT].consultation
      (nrn,
       requester_reference,
       first_name,
       last_name,
       consultation_number,
       requester_id,
       request_date,
       created_on,
       created_by_organization_name,
       created_by_organization_id,
       created_by_user_name,
       created_by_user_id,
       justification_id,
       consultation_map,
       party_map)
      SELECT t2.identificationnumber,
             t1.dossierreference                           AS requester_ref,
             t2.firstnames,
             t2.lastnames,
             t1.number,
             (SELECT requester_id
              FROM [CRT].consultation_requester ts
              WHERE ts.requester_map_id = t1.abstractrequest_id),
             t1.requestreceiveddate,
             t1.requestreceiveddate,
             NULL                                          AS creatorstudyname,
             t1.creatorstudyid,
             t1.creatorname,
             t1.creatorpersonid,
             (SELECT justification_id
              FROM [CRT].justification
              WHERE consultation_map = abstractrequest_id) AS justif_id,
             t1.abstractrequest_id,
             t2.involvedparty_id
      FROM [CRTCRH].[contractregistrationservice].crs.searchrequest t1
      --FROM [contractregistrationservice].crs.searchrequest t1
             INNER JOIN [CRTCRH].[contractregistrationservice].crs.involvedparty t2
             --INNER JOIN [contractregistrationservice].crs.involvedparty t2
                        ON t1.firstparty_id = t2.involvedparty_id
      WHERE t1.requesttype = 'SEARCHTAD'

      INSERT INTO [CRT].crhlink
      SELECT paper_deed_id,
             mapid,
             marriagecontractregistration_id
      FROM [CRT].juridical_deed
             INNER JOIN
           [CRTCRH].[contractregistrationservice].crs.marriagecontractregistration_testamentregistration
           --[contractregistrationservice].crs.marriagecontractregistration_testamentregistration
           ON mapid = testamentregistration_id
      WHERE paper_deed_id NOT IN (SELECT paper_deed_id
                                  FROM [CRT].crhlink)

*/
    INSERT INTO [CRT].updatedate(created_on, register) values (sysdatetime(), 'CRH')


    exec [migration].enableAllForeignKeys

  end try

  begin catch
    declare @error int, @message varchar(4000), @xstate int;
    SELECT @error = ERROR_NUMBER(), @message = ERROR_MESSAGE(), @xstate = XACT_STATE();

    print 'Error# ' + cast(@error as varchar) + ' , state=' + cast(@xstate as varchar) + ', message=' + @message
    print 'Actual trancount=' + cast(@@TRANCOUNT as varchar) + ', @trancount=' + cast(@trancount as varchar)

    if @xstate = -1 -- The current request has an active user transaction, but an error has occurred
      -- that has caused the transaction to be classified as an uncommittable transaction
      begin
        rollback
      end
    if @xstate = 1 --AND @trancount = 0 -- Here we will rollback all this procedure and there were no other transaction open
      -- before the starting of this procedure for @trancount = 0
      begin
        rollback
      end
    /*if @xstate = 1 AND @trancount > 0 -- Here we will rollback only the modification of this procedure
      -- but not the modifications of the procedure that calls this one for, if @trancount > 0,
      -- it means that there were already a transaction open before the starting of this procedure.
      begin
        rollback transaction csid
      end;*/

    set @message = 'inside migration batch: '+ ERROR_MESSAGE();
    THROW 50003, @message, 1
  end catch

END
GO

</pre>



</template>