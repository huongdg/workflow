from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import date
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import date
from airflow.sensors.time_delta import TimeDeltaSensor

# Create Dag
dag = DAG('C360_monthly_nhandt', description='C360_monthly',
          schedule_interval="0 3 * * *",
          start_date=datetime(2022, 9, 30),
          catchup=False)

# Parameter
month = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=1)).strftime('%Y%m')}}"
day = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=1)).strftime('%Y%m%d')}}"
day2 = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=2)).strftime('%Y%m%d')}}"
day3 = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=3)).strftime('%Y%m%d')}}"

local_path_50_pre = '/u33/dev/tuannn/deploy/mytv/job_script/pre'
local_path_50_main = '/u33/dev/tuannn/deploy/mytv/job_script/main'
local_path_51 = '/u30/mytv/nntuann/deploy/job_script'

with dag:
    """ Function delay time """
    delay_3h = TimeDeltaSensor(
        task_id='delay_3h',
        delta=timedelta(hours=3),
        poke_interval=60 * 60,  # 1 hours
        queue='bigdata50',
        mode='reschedule'
    )

    delay_4h = TimeDeltaSensor(
        task_id='delay_4h',
        delta=timedelta(hours=4),
        poke_interval=60 * 60 * 2,  # 2 hours
        queue='bigdata50',
        mode='reschedule'
    )

    delay_8h = TimeDeltaSensor(
        task_id='delay_8h',
        delta=timedelta(hours=8),
        poke_interval=60 * 60 * 2,  # 2 hours
        queue='bigdata50',
        mode='reschedule'
    )

    delay_9h = TimeDeltaSensor(
        task_id='delay_9h',
        delta=timedelta(hours=9),
        poke_interval=60 * 60 * 3,  # 3 hours
        queue='bigdata50',
        mode='reschedule'
    )

    """ Function Import Raw """
    imp_hbo_go = BashOperator(
        task_id='imp_hbo_go',
        bash_command='bash ' + local_path_51 + '/importHBO.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_bundle_mobi = BashOperator(
        task_id='imp_bundle_mobi',
        bash_command='bash ' + local_path_51 + '/importBundleMobi.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_bundle_vina = BashOperator(
        task_id='imp_bundle_vina',
        bash_command='bash ' + local_path_51 + '/importBundleVina.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_add_on_bhd = BashOperator(
        task_id='imp_add_on_bhd',
        bash_command='bash ' + local_path_51 + '/importAddonBhd.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_add_on_cab = BashOperator(
        task_id='imp_add_on_cab',
        bash_command='bash ' + local_path_51 + '/importAddonCab.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_add_on_clg = BashOperator(
        task_id='imp_add_on_clg',
        bash_command='bash ' + local_path_51 + '/importAddonClg.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_add_on_fim = BashOperator(
        task_id='imp_add_on_fim',
        bash_command='bash ' + local_path_51 + '/importAddonFim.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_add_on_kplus = BashOperator(
        task_id='imp_add_on_kplus',
        bash_command='bash ' + local_path_51 + '/importAddonKplus.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_pay_lock = BashOperator(
        task_id='imp_pay_lock',
        bash_command='bash ' + local_path_51 + '/importPayLock.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_contract = BashOperator(
        task_id='imp_contract',
        bash_command='bash ' + local_path_51 + '/importContract.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_providers = BashOperator(
        task_id='imp_providers',
        bash_command='bash ' + local_path_51 + '/importProviders.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    imp_member_login = BashOperator(
        task_id='imp_member_login',
        bash_command='bash ' + local_path_51 + '/importMemberLogin.sh ' + day + ' ',
        queue='bigdata51',
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    """ Function Preprocessing """

    pre_u_content_vmp_1 = BashOperator(
        task_id='pre_u_content_vmp_1',
        bash_command='bash ' + local_path_50_pre + '/runContent1.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_u_content_vmp_2 = BashOperator(
        task_id='pre_u_content_vmp_2',
        bash_command='bash ' + local_path_50_pre + '/runContent2.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_u_content_vmp_3 = BashOperator(
        task_id='pre_u_content_vmp_3',
        bash_command='bash ' + local_path_50_pre + '/runContent3.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    clean_customer = BashOperator(
        task_id='pre_d_clean_customer',
        bash_command='bash ' + local_path_50_pre + '/cleanCustomer.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    clean_member_login = BashOperator(
        task_id='pre_d_clean_member_login',
        bash_command='bash ' + local_path_50_pre + '/cleanMemberLogin.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    time_slot_access = BashOperator(
        task_id='pre_time_slot_access',
        bash_command='bash ' + local_path_50_pre + '/timeSlotAccess.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    traffic_live = BashOperator(
        task_id='pre_traffic_live',
        bash_command='bash ' + local_path_50_pre + '/trafficLive.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    traffic_vod = BashOperator(
        task_id='pre_traffic_vod',
        bash_command='bash ' + local_path_50_pre + '/trafficVod.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    watch_by_cate_vmp = BashOperator(
        task_id='pre_watch_by_cate_vmp',
        bash_command='bash ' + local_path_50_pre + '/watchByCateVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    watch_by_cate_zte = BashOperator(
        task_id='pre_watch_by_cate_zte',
        bash_command='bash ' + local_path_50_pre + '/watchByCateZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    watch_by_channel_zte = BashOperator(
        task_id='pre_watch_by_channel_zte',
        bash_command='bash ' + local_path_50_pre + '/watchByChannelZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    watch_by_channel_vmp = BashOperator(
        task_id='pre_watch_by_channel_vmp',
        bash_command='bash ' + local_path_50_pre + '/watchByChannelVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    watch_by_content_vmp = BashOperator(
        task_id='pre_watch_by_content_vmp',
        bash_command='bash ' + local_path_50_pre + '/watchByContentVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    watch_by_content_zte = BashOperator(
        task_id='pre_watch_by_content_zte',
        bash_command='bash ' + local_path_50_pre + '/watchByContentZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_m_summary_service = BashOperator(
        task_id='pre_m_summary_service',
        bash_command='bash ' + local_path_50_pre + '/summaryService.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_m_summary_vod_vmp = BashOperator(
        task_id='pre_m_summary_vod_vmp',
        bash_command='bash ' + local_path_50_pre + '/smrVodVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_device_use_vmp = BashOperator(
        task_id='pre_d_device_use_vmp',
        bash_command='bash ' + local_path_50_pre + '/getDeviceUseVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_exp_charge = BashOperator(
        task_id='pre_d_mobile_exp_charge',
        bash_command='bash ' + local_path_50_pre + '/mobileExportCharge.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_addon = BashOperator(
        task_id='pre_d_mobile_addon',
        bash_command='bash ' + local_path_50_pre + '/mobileAddon.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_bundle_detail = BashOperator(
        task_id='pre_d_mobile_bundle_detail',
        bash_command='bash ' + local_path_50_pre + '/mobileBundleDetail.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_single_detail = BashOperator(
        task_id='pre_d_mobile_single_detail',
        bash_command='bash ' + local_path_50_pre + '/mobileSingleDetail.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_customer_all = BashOperator(
        task_id='pre_d_mobile_customer_all',
        bash_command='bash ' + local_path_50_pre + '/mobileCustomerAll.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_info_cust = BashOperator(
        task_id='pre_d_mobile_info_cust',
        bash_command='bash ' + local_path_50_pre + '/mobileInfoCust.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_mobile_customer_fee = BashOperator(
        task_id='pre_d_mobile_customer_fee',
        bash_command='bash ' + local_path_50_pre + '/mobileCustomerFee.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_addon_bhd = BashOperator(
        task_id='pre_d_addon_bhd',
        bash_command='bash ' + local_path_50_pre + '/addonBhd.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_addon_clg = BashOperator(
        task_id='pre_d_addon_clg',
        bash_command='bash ' + local_path_50_pre + '/addonCLG.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_addon_fim = BashOperator(
        task_id='pre_d_addon_fim',
        bash_command='bash ' + local_path_50_pre + '/addonFim.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_addon_hbo = BashOperator(
        task_id='pre_d_addon_hbo',
        bash_command='bash ' + local_path_50_pre + '/addonHbo.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_addon_kplus = BashOperator(
        task_id='pre_d_addon_kplus',
        bash_command='bash ' + local_path_50_pre + '/addonKplus.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    pre_d_addon_vtvcab = BashOperator(
        task_id='pre_d_addon_vtvcab',
        bash_command='bash ' + local_path_50_pre + '/addonVtvCab.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    """ Function main"""

    m_interaction_vmp = BashOperator(
        task_id='m_interaction_vmp',
        bash_command='bash ' + local_path_50_main + '/interactionVMPmonthly.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_interaction_zte = BashOperator(
        task_id='m_interaction_zte',
        bash_command='bash ' + local_path_50_main + '/interactionZTEmonthly.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_interaction_all = BashOperator(
        task_id='m_interaction_all',
        bash_command='bash ' + local_path_50_main + '/interactionALLmonthly.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_summary_service = BashOperator(
        task_id='m_summary_service',
        bash_command='bash ' + local_path_50_main + '/summaryService.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_interaction_vmp = BashOperator(
        task_id='d_interaction_vmp',
        bash_command='bash ' + local_path_50_main + '/interactionVMPdaily.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_interaction_zte = BashOperator(
        task_id='d_interaction_zte',
        bash_command='bash ' + local_path_50_main + '/interactionZTEdaily.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_interaction_all = BashOperator(
        task_id='d_interaction_all',
        bash_command='bash ' + local_path_50_main + '/interactionALLdaily.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_content_not_seen = BashOperator(
        task_id='d_content_not_seen',
        bash_command='bash ' + local_path_50_main + '/contentNotSeen.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_content_complete = BashOperator(
        task_id='d_content_complete',
        bash_command='bash ' + local_path_50_main + '/contentComplete.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_content_active_vmp = BashOperator(
        task_id='d_content_active_vmp',
        bash_command='bash ' + local_path_50_main + '/contentActiveVMP.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_content_active_zte = BashOperator(
        task_id='d_content_active_zte',
        bash_command='bash ' + local_path_50_main + '/contentActiveZTE.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_top_content = BashOperator(
        task_id='m_top_content',
        bash_command='bash ' + local_path_50_main + '/topContent.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_change_view_by_cate = BashOperator(
        task_id='m_change_view_by_cate',
        bash_command='bash ' + local_path_50_main + '/changeViewByCate.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_subs_by_timeline_vmp = BashOperator(
        task_id='m_subs_by_timeline_vmp',
        bash_command='bash ' + local_path_50_main + '/subsByTimelineVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_subs_by_timeline_zte = BashOperator(
        task_id='m_subs_by_timeline_zte',
        bash_command='bash ' + local_path_50_main + '/subsByTimelineZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_summary_live_vmp = BashOperator(
        task_id='m_summary_live_vmp',
        bash_command='bash ' + local_path_50_main + '/smrLiveVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_summary_live_zte = BashOperator(
        task_id='m_summary_live_zte',
        bash_command='bash ' + local_path_50_main + '/smrLiveZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_summary_vod_zte = BashOperator(
        task_id='m_summary_vod_zte',
        bash_command='bash ' + local_path_50_main + '/smrVodZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_summary_vod_vmp = BashOperator(
        task_id='m_summary_vod_vmp',
        bash_command='bash ' + local_path_50_main + '/smrVodVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_addon_use_active = BashOperator(
        task_id='m_addon_use_active',
        bash_command='bash ' + local_path_50_main + '/addonActiveUser.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_addon_report = BashOperator(
        task_id='m_addon_report',
        bash_command='bash ' + local_path_50_main + '/addonReportMonthly.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_addon_report = BashOperator(
        task_id='d_addon_report',
        bash_command='bash ' + local_path_50_main + '/addonReportDaily.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_mobile_popup = BashOperator(
        task_id='d_mobile_popup',
        bash_command='bash ' + local_path_50_main + '/mobilePopup.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_mobile_behavior_age_sex = BashOperator(
        task_id='d_mobile_behavior_age_sex',
        bash_command='bash ' + local_path_50_main + '/mobileBehaviorAgeSex.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_count_log_vmp = BashOperator(
        task_id='m_count_log_vmp',
        bash_command='bash ' + local_path_50_main + '/countLogVmp.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    m_count_log_zte = BashOperator(
        task_id='m_count_log_zte',
        bash_command='bash ' + local_path_50_main + '/countLogZte.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_vmp_vod_by_age_sex = BashOperator(
        task_id='d_vmp_vod_by_age_sex',
        bash_command='bash ' + local_path_50_main + '/vmpVodByAgeSex.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_vmp_movie_by_age_sex = BashOperator(
        task_id='d_vmp_movie_by_age_sex',
        bash_command='bash ' + local_path_50_main + '/vmpMovieByAgeSex.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_zte_vod_by_age_sex = BashOperator(
        task_id='d_zte_vod_by_age_sex',
        bash_command='bash ' + local_path_50_main + '/zteVodByAgeSex.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    d_live_by_age_sex = BashOperator(
        task_id='d_live_by_age_sex',
        bash_command='bash ' + local_path_50_main + '/liveByAgeSex.sh ' + day + ' ',
        queue='bigdata50',
        retries=0
    )

    # DAG
    delay_3h >> [imp_add_on_bhd, imp_add_on_kplus, imp_add_on_cab, imp_add_on_clg, imp_add_on_fim, imp_pay_lock,
                 imp_bundle_mobi, imp_contract, imp_providers, imp_member_login]

    imp_add_on_bhd >> pre_d_addon_bhd

    imp_add_on_kplus >> pre_d_addon_kplus

    imp_add_on_cab >> pre_d_addon_vtvcab

    imp_add_on_clg >> pre_d_addon_clg

    imp_add_on_fim >> pre_d_addon_fim

    delay_8h >> [imp_bundle_vina, imp_hbo_go]

    imp_hbo_go >> pre_d_addon_hbo

    delay_9h >> pre_d_mobile_exp_charge >> [pre_d_mobile_single_detail, pre_d_mobile_addon]

    [imp_bundle_vina, imp_bundle_mobi] >> pre_d_mobile_bundle_detail

    [pre_d_mobile_addon, pre_d_addon_bhd, pre_d_addon_hbo, pre_d_addon_fim, pre_d_addon_vtvcab, pre_d_addon_clg,
     pre_d_addon_kplus, pre_d_device_use_vmp] >> m_addon_use_active

    [pre_d_mobile_addon, pre_d_addon_bhd, pre_d_addon_hbo, pre_d_addon_fim, pre_d_addon_vtvcab, pre_d_addon_clg,
     pre_d_addon_kplus] >> d_addon_report

    [pre_d_mobile_addon, pre_d_addon_bhd, pre_d_addon_hbo, pre_d_addon_fim, pre_d_addon_vtvcab, pre_d_addon_clg,
     pre_d_addon_kplus] >> m_addon_report

    [pre_d_mobile_exp_charge, pre_d_mobile_bundle_detail] >> pre_d_mobile_customer_all >> [pre_d_mobile_customer_fee,
                                                                                           pre_d_mobile_info_cust]

    [pre_d_mobile_info_cust, watch_by_content_vmp] >> d_mobile_behavior_age_sex

    [pre_d_mobile_customer_all, pre_d_mobile_bundle_detail] >> d_mobile_popup

    pre_u_content_vmp_1 >> [pre_u_content_vmp_2, watch_by_cate_vmp]
    pre_u_content_vmp_2 >> pre_u_content_vmp_3 >> [watch_by_content_vmp, traffic_vod,
                                                   d_content_active_vmp, m_subs_by_timeline_vmp]

    delay_8h >> [watch_by_content_zte, watch_by_cate_zte, watch_by_channel_zte, d_content_active_zte,
                 m_subs_by_timeline_zte, m_count_log_zte, time_slot_access]

    [pre_u_content_vmp_3, delay_8h] >> d_content_complete

    [watch_by_content_zte, watch_by_content_vmp] >> pre_m_summary_service >> m_summary_service

    [pre_m_summary_service, watch_by_content_vmp] >> m_interaction_vmp

    [pre_m_summary_service, watch_by_content_zte] >> m_interaction_zte

    [m_interaction_vmp, m_interaction_zte] >> m_interaction_all

    watch_by_content_zte >> [d_interaction_zte, m_summary_live_zte, m_summary_vod_zte]

    watch_by_content_vmp >> d_interaction_vmp

    [d_interaction_zte, d_interaction_vmp] >> d_interaction_all

    [watch_by_content_zte, watch_by_content_vmp] >> d_content_not_seen

    [traffic_vod, watch_by_content_vmp] >> pre_m_summary_vod_vmp >> [m_top_content, m_change_view_by_cate,
                                                                     m_summary_vod_vmp]
    [traffic_live, watch_by_content_vmp] >> m_summary_live_vmp

    imp_member_login >> clean_member_login >> clean_customer

    [clean_customer, watch_by_content_vmp] >> d_vmp_vod_by_age_sex
    [clean_customer, watch_by_content_vmp] >> d_vmp_movie_by_age_sex
    [clean_customer, watch_by_content_zte] >> d_zte_vod_by_age_sex
    [clean_customer, watch_by_content_zte, watch_by_content_vmp] >> d_live_by_age_sex
