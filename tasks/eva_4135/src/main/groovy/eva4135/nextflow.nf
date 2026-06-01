nextflow.enable.dsl=2

params.max_parallel_chunks    = 5
params.project_dir            = null
params.groovy_script          = null
params.working_dir            = null
params.env_properties_file    = null
params.assembly_report_dir    = null

def dbNames = ['eva_Eaasinus_equasi10','eva_aaegypti_aaegl2','eva_aarabiensis_dong5av1','eva_accession_human_sharded',
'eva_accession_sharded','eva_achinensis_red5ps11690','eva_achrysaetos_aquilachrysaetos102','eva_achukar_chukar10',
'eva_acomosus_asm154086v1','eva_aculicifacies_a371v1','eva_acygnoides_anscygv10','eva_acygnoides_goosev10',
'eva_adigitata_asm2944870v1','eva_aepiroticus_epiroticus2v1','eva_afarauti_far1v2','eva_aflavus_asm1411746v1',
'eva_afunestus_fumozv1','eva_afunestus_idanofuneda41604','eva_agambiae_agamp3','eva_agambiaestrpest_agamp3',
'eva_aglabripennis_agla20','eva_ahypogaea_arahytifrunnergnm1kyv3','eva_amelas_cm1001059av2','eva_amellifera_amelhav31',
'eva_amexicanum_asm291563v1','eva_aminimus_1v1','eva_apalmata_adig11','eva_aphrygia_10','eva_aplatyrhynchos_cauwild10',
'eva_aplatyrhynchos_iascaaspbh15','eva_aquadriannulatus_quad4av1','eva_arufa_asm94733150v1','eva_asapidissima_falosap1pri',
'eva_asinensis_v1','eva_astephensi_sda500v1','eva_aterreus_asm3464202v1','eva_athaliana_tair10','eva_athaliana_tair101',
'eva_banthracis_asm784v1','eva_bbison_arsucd13','eva_bbison_nddbsh1','eva_bbison_umd31','eva_bbonasus_umd311',
'eva_bbubalis_umdcaspurwb20','eva_bbubalis_uoa_wb_1','eva_bgrunniens_bosgru30','eva_bgrunniens_lubosgruv30',
'eva_bgrunniens_umd311','eva_bindicus_arsucd12','eva_bindicus_arsucd13','eva_bindicus_bosindicus10','eva_bindicus_btau501',
'eva_bindicus_umd31','eva_bjuncea_t8466v1','eva_bmutus_bosgruv20','eva_bnapus_branapusv20','eva_boleracea_BOL',
'eva_bos_arsucd12','eva_bos_arsucd20','eva_btaurus_arsucd12','eva_btaurus_arsucd13','eva_btaurus_arsucd20',
'eva_btaurus_btau501','eva_btaurus_umd31','eva_btaurus_umd311','eva_c_asm51225v2','eva_cannuum_ucd10xv11',
'eva_cannuum_zunla1ref10','eva_carabica_asm4911477v1','eva_carietinum_ASM33114v1','eva_ccajan_10',
'eva_ccalophylla_asm1418284v1','eva_cdromedarius_camdro2','eva_cdromedarius_camdro3','eva_celegans_wbcel235',
'eva_cesculenta_tarojaasv10','eva_cfamiliaris_31','eva_cfamiliaris_cfamiliaris20','eva_cfamiliaris_dog10kboxertasha',
'eva_cfamiliaris_roscfam10','eva_cfamiliaris_uucfamgsd10','eva_charengus_chv202','eva_chircus_10','eva_chircus_ars1',
'eva_chircus_ars12','eva_chircus_asm4283598v1','eva_chircus_t2tgoat10','eva_cjacchus_32','eva_cmollissima_cmv2',
'eva_cpepo_asm280686v2','eva_cporcellus_30','eva_cpunctiferalis_asm3116337v1','eva_cpurpureus_cpurpureuscvpurple',
'eva_cquilicii_ccap21','eva_csabaeus_chlsab11','eva_csativa_asm186575v1','eva_csativa_cs10_2','eva_csativus_v3',
'eva_cvirginica_cvirginica30','eva_ddiscoideum_dicty27','eva_ddugon_asm4018294v1','eva_dexilis_foniocm05836',
'eva_dhelianthi_dhel01v2','eva_dlabrax_dlabrax2021','eva_dlabrax_seabassv10','eva_dmelanogaster_6','eva_dpipra_asm171598v1',
'eva_drerio_grcz10','eva_drerio_grcz11','eva_e_30','eva_e_asm1607732v2','eva_ecaballus_20','eva_ecaballus_30',
'eva_ecaballus_equcabfriesianwur','eva_edunnii_egrandis20','eva_eoleiferaxeguinessnsis_EG5','eva_epellita_egrandis20',
'eva_etef_salkteffdabbi30','eva_f_fcatusfca126mat10','eva_f_ptigrispti1mat11','eva_falbicollis_15','eva_fcatus_80',
'eva_fcatus_90','eva_fcatus_fcatusfca126mat10','eva_fcatus_felcat91x','eva_fexcelsior_batg05','eva_foxysporum_ii5v1',
'eva_fsylvatica_bhagachr','eva_g_fgouwil21','eva_gfuscipes_yalegfus2','eva_ggallus_bgalgal1matbroilergrcg7b',
'eva_ggallus_galgal5','eva_ggallus_grcg6a','eva_ghirsutum_gossypiumhirsutumv21','eva_gmax_20','eva_gmax_gmaxv11',
'eva_gmax_v1','eva_gmax_v21','eva_h_grch38','eva_h_morexv3pseudomoleculesassembly','eva_hannuus_xrqr10',
'eva_hbrasiliensis_asm165405v1','eva_hchromini_Orenil11','eva_hglaber_hglaberassemblyv10','eva_hglaber_nakedmoleratmaternal',
'eva_hglaber_nakedmoleratpaternal','eva_hillucens_iherill22curated20191125','eva_hleucocephalus_40','eva_hpylori_asm852v1',
'eva_hsapiens_asm240226v1','eva_hsapiens_grch37','eva_hsapiens_grch38','eva_hsapiens_t2tchm13v20',
'eva_hsapiens_t2tchm13v20withmaskedyandrcrs','eva_hvulgare_assemblyforbarleycultivarmorexv10','eva_hvulgare_barleyshapebarke',
'eva_hvulgare_morexv10updatex','eva_hvulgare_morexv20','eva_hvulgare_morexv20test0h',
'eva_hvulgare_morexv3pseudomoleculesassembly','eva_jregia_walnut20','eva_jregia_wgs5d','eva_lalbus_cnrslalb10',
'eva_langustifolius_lupangtanjilv10','eva_lcatta_mmur30','eva_lmonocytogenesegde_asm19603v1','eva_lpolyactis_asm1011929v1',
'eva_lrohita_asm412021v1','eva_lsalmonis_lsalatlcanadafemalev1','eva_lsativa_lsatsalinasv7','eva_lsativa_lsatsalinasv8',
'eva_lusitatissimum_asm22429v2','eva_mangustirostris_asm2128878v3','eva_mchrysops_dom152mochry10','eva_mdomestica_asm211411v1',
'eva_metadata','eva_mgallopavo_50','eva_mgigas_asm1103280v1','eva_mindica_catasmindica21','eva_mmulatta_801',
'eva_mmulatta_mmul10','eva_mmusculus_grcm38','eva_mmusculus_grcm39','eva_mmusculus_mgscv37','eva_mnorvegica_mnorhav2',
'eva_mopercularis_macope2','eva_msubspparatuberculosis_asm786v1','eva_mtetraphylla_scumintv3','eva_ntabacum_nitab45',
'eva_nvison_nnqggv1','eva_oammon_nipbtibetanargali10','eva_oanatinus_501','eva_oaries_arsuirambv20','eva_oaries_arsuirambv30',
'eva_oaries_cauoaries10','eva_oaries_oarrambouilletv10','eva_oaries_oarv31','eva_oaries_oarv40','eva_ocuniculus_20',
'eva_odallidalli_oarv31','eva_odigyna_cnuodkoprichrhap110','eva_oeuropaea_asm4816504v1','eva_oeuropaea_asm4816919v1',
'eva_omykiss_usdaomyka11','eva_oniloticus_umdnmbu','eva_orhinoceros_s47k2v3','eva_orufipogon_asm3799707v1','eva_orufipogon_irgsp10',
'eva_osativa_irgsp10','eva_osativa_osativa40','eva_osativaindicagroup_irgsp10','eva_osativaindicagroup_r498genomeversion1',
'eva_osativajaponicagroup_irgsp10','eva_osativajaponicagroup_osativa40','eva_pabies_a541150contigsfastagz','eva_pbairdii_hupman21',
'eva_pdabryanus_pdcontigs10','eva_pfalciparum3D7_GCA000002765','eva_pfalciparum_GCA000002765','eva_pfalciparum_asm276v2',
'eva_pgraminisfsptritici_ksupgt99ks76a20','eva_pguajava_guavav1123','eva_pinnocua_avenasativacvsangv1assembly','eva_plunatus_colplunatus10',
'eva_pmajor_11','eva_pputidakt2440_asm756v2','eva_prubi_pr4671','eva_psativum_jicpsatv13','eva_pstriiformis_pst134e36v1pri',
'eva_ptremula_potraassembly1','eva_pvivax_gca900093555','eva_pvivax_pvp01','eva_pvivax_pvpam','eva_pvulgaris_10','eva_pyedoensis_pynv1',
'eva_rnorvegicus_60','eva_rnorvegicus_mratbn72','eva_sarscov2_asm985889v3','eva_saureus_asm3748308v1','eva_sbicolor_ncbiv3',
'eva_sbicolor_sorbi1','eva_scereale_ryelo72018v1p1p1','eva_scerevisiae_r64','eva_sdumerili_10','eva_sfrugiperda_zjusfru11',
'eva_sitalica_setariav1','eva_slucioperca_slucfbn12','eva_slycopersicum_sl240','eva_slycopersicum_sl250','eva_smacromomyceticus_20',
'eva_smansoni_23792v2','eva_spombe_asm294v2','eva_sratti_ed321v504','eva_ssalar_20','eva_ssalar_ssalv3','eva_ssalar_ssalv31',
'eva_ssclerotiorum1980uf70_asm185786v1','eva_sscrofa_102','eva_sscrofa_111','eva_t_iwgsccsrefseqv21',
'eva_taestivum_eitriticumaestivumcadenzav2','eva_taestivum_eitriticumaestivumparagonv3','eva_taestivum_iwgsccsrefseqv21',
'eva_taestivum_iwgscrefseqv10','eva_tcacao_20110822','eva_tcacao_criollococoagenomev2','eva_tcastaneum_tcas52',
'eva_tdicoccoides_wewseqv1','eva_tgrandiflorum_criollococoagenomev2','eva_tguttata_324','eva_vpacos_202','eva_vpacos_vicpac31',
'eva_vpacos_vicpac32','eva_vunguiculata_asm411807v1','eva_vunguiculata_asm411807v2','eva_vvinifera_12x','eva_vvulpes_vulvul22',
'eva_zmays_agpv2','eva_zmays_agpv3','eva_zmays_agpv4','eva_zmays_zmb73referencenam50','eva_zmayssubspmays_agpv3',
'eva_zmayssubspmays_agpv4','eva_zmobilis_asm710v1'
]

process REMEDIATE_DB {
    label 'long_time'
    label 'med_mem'
    maxForks params.max_parallel_chunks

    tag { db_name }

    input:
    val db_name

    output:
    path "${db_name}.done", emit: done_flag

    script:
    """
    bash run_groovy_script.sh \
        ${params.project_dir} \
        ${params.groovy_script} \
        -workingDir=${params.working_dir} \
        -envPropertiesFile=${params.env_properties_file} \
        -assemblyReportDir=${params.assembly_report_dir} \
        -dbName=${db_name} \
        > ${params.working_dir}/${db_name}.log 2>&1

    echo "Done: ${db_name}" > ${db_name}.done
    """
}

workflow {
    if (!params.project_dir)         error "Please provide --project_dir"
    if (!params.groovy_script)       error "Please provide --groovy_script"
    if (!params.working_dir)         error "Please provide --working_dir"
    if (!params.env_properties_file) error "Please provide --env_properties_file"
    if (!params.assembly_report_dir) error "Please provide --assembly_report_dir"

    REMEDIATE_DB(Channel.fromList(dbNames))

    REMEDIATE_DB.out.done_flag
        .collect()
        .view { flags -> "All DBs complete: ${flags.size()} DBs processed" }
}