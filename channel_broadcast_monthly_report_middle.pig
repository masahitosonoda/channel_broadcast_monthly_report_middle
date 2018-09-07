/****************************************************************************/
%DECLARE TITLE '動画広告レポート（月次）';
/****************************************************************************
■概要
    テレビ局などがスポンサー/代理店向けに配信実績報告書やマーケティング情報として提出するために
    ニコニコチャンネルで広告配信された番組・動画に関する再生数やUU数、デモグラフィックスの集計を行う。

    最終集計結果はこのpigでは出さないで、結果算出用のデータを中間データを出力する

    利用者向けの説明は以下を参照。
    https://atl.dwango.co.jp/confluence/x/jTPNAw

■コンテンツリストファイル
    広告の対象となる番組・タイトルのリスト。利用者が作成し予めホームディレクトリにアップロードしておく。

■引数
    $START_DATE      :集計開始日 [YYMMDD]
    $END_DATE        :集計終了日 [YYMMDD]
    $INFILE0         :コンテンツリストファイルのパス名 [hdfs_path]
        (default=channel_broadcast_contents.txt)
    $REPORT_TO       :報告先の名前
        (default=to_default)

■出力
    11種類のレポートが出力される。
        (hdfs_path=video_ad_report/"$REPORT_TO"/monthly/"$START_DATE"_"$END_DATE"/report_XX)
    report_01:特集ページ実績
    report_02:再生ページ実績
    report_03:番組別視聴数
    report_04:再生ページの時間帯別実績
    report_05:特集ページのデモグラフィクス
    report_06:再生ページのデモグラフィクス
    report_07:各番組視聴者のデモグラフィクス
    report_08:特集ページの視聴エリア
    report_09:再生ページの視聴エリア
    report_10:視聴残存率
    report_11:新規再生・再訪問再生

*****************************************************************************/
%DEFAULT LIB_ROOT 'hdfs://BigBrotherHA'
IMPORT '${LIB_ROOT}/lib/latest/pig/bblogload.pig';
IMPORT '${LIB_ROOT}/user/masahito_sonoda/files/pig/make_reports.pig';

--------------------------------------------------------------
-- UDFs
--------------------------------------------------------------
REGISTER ${LIB_ROOT}/lib/latest/pig/PigLib.jar;
REGISTER ${LIB_ROOT}/lib/latest/pig/PigUdf.jar;
DEFINE ConvertDateToUnixtimeMillis  jp.co.dwango.bigbrother.datetime.ConvertDateToUnixtimeMillis();
DEFINE SplitRequest                 jp.co.dwango.bigbrother.string.SplitRequest();
DEFINE SplitUri                     jp.co.dwango.dataplat.udf.pig.SplitUri();
DEFINE SplitQueryString             jp.co.dwango.bigbrother.string.SplitQueryString();
DEFINE ConvertAgeToAgeGroup         jp.co.dwango.bigbrother.user.ConvertAgeToAgeGroup();
DEFINE GetDevice                    jp.co.dwango.bigbrother.ua.GetDevice();
DEFINE getDeviceSystemType          jp.co.dwango.bigbrother.udf.pig.GetDeviceBySystemType();


-------------------------------------------------------------
-- VARIABLES
-------------------------------------------------------------
-- 日付関連
%DEFAULT START_DATE                 '150413';
%DEFAULT END_DATE                   '150414';
%DEFAULT YESTERDAY                  `date -d "1 day ago" +"%y%m%d"`;
%DECLARE START_DATE_TIMESTAMP       `date -d $START_DATE +"%s000L"`;
%DECLARE END_DATE_TIMESTAMP         `date -d "1 day $END_DATE" +"%s000L"`;
%DECLARE LAST_START_DATE            `date -d "1 day ago $START_DATE" +"%y%m%d"`;
%DECLARE LOG_START_DATE             '$START_DATE';
%DEFAULT LOG_END_DATE               `date -d "$YESTERDAY" +"%Y%m%d"`
%DECLARE LAST_LOG_START_DATE        `date -d "1 day ago $LOG_START_DATE" +"%Y%m%d"`
%DECLARE LOAD_DAYS `
  now_date=$LAST_START_DATE
  while ((now_date<=$END_DATE));do
    echo -n \${now_date},
    now_date=$(date -d "\${now_date} 1 day" +%y%m%d)
  done
`;
%DECLARE WATCH_SCREENMODE_LOAD_DAYS `
  now_date=$LAST_LOG_START_DATE
  while ((now_date<=$LOG_END_DATE));do
    echo -n \${now_date},
    now_date=$(date -d "\${now_date} 1 day" +%Y%m%d)
  done
`;
%DECLARE LAST_APRIL2ND_DATETIME `
  # \${arg1}直前の4月2日をiso8601で出力する(学年を求めたいので)
  arg1="$START_DATE"
  target_year=$(date -d "\${arg1} 3 months ago" +%y)
  date --iso-8601=s -d "\${target_year}0402"
`;
-- 入出力パス
%DEFAULT REPORT_TO 'to_default';
%DEFAULT INFILE0 'channel_broadcast_contents.txt'
%DECLARE OUTDIR01 `echo "video_ad_report/"$REPORT_TO"/monthly/"$START_DATE"_"$END_DATE"/middle_log"`;

-- ジョブ名
SET job.name '$TITLE -p START_DATE=$START_DATE -p END_DATE=$END_DATE -p LOG_END_DATE=$LOG_END_DATE -p REPORT_TO=$REPORT_TO';
-- 集約基準、ソート基準の定義
%DECLARE GROUP_KEY_01   'access_date, screen_name, channel_name, device';
%DECLARE GROUP_KEY_05   'access_date, screen_name, channel_name, device, sex, social_category';
%DECLARE GROUP_KEY_08   'access_date, screen_name, channel_name, device, prefecture';
%DECLARE SORT_KEY_01    'screen_name, access_date';
%DECLARE SORT_KEY_05    'screen_name, access_date, sex, social_category';
%DECLARE SORT_KEY_08    'screen_name, access_date, prefecture';


-------------------------------------------------------------
-- LOAD from BB
-- ６種類のデータの読み込み
-- L1) 集計対象のコンテンツ（動画）のリスト（ユーザ作成）：　集計用contents_listとchannel_listを作る
-- L2) ユーザー属性情報: ユーザの属性情報のuser_tableを作る
-- L3) 動画情報：　コンテンツの属性情報、およびスレッドIDの変換に利用するためにvideo_tableを作る
-- L4) 再生ページ集計用 apache ログ (WatchScreenModeLog)
-- L5) 特集ページ集計用 apache ログ (channel_apache_pc)
-------------------------------------------------------------
-- L1) ユーザ作成のコンテンツ・リスト読み込み
contents_list_00 = LOAD '$INFILE0';
contents_list_10 = FOREACH contents_list_00 GENERATE
    (chararray)$0   AS screen_name,
    (chararray)$1   AS channel_name,
    (chararray)$2   AS program_name,
    (chararray)$3   AS contents_id,
    (chararray)$4   AS title,
    (chararray)$5   AS broadcast_start_datetime,
    (chararray)$6   AS broadcast_end_datetime;

-- ヘッダー行を読み飛ばす目的
contents_list_20 = FILTER contents_list_10 BY
    contents_id != 'contents_id'
AND
    screen_name != 'screen_name';

-- 集計用のコンテンツリストの作成
contents_list = FOREACH contents_list_20 GENERATE
    screen_name,
    channel_name,
    program_name,
    contents_id,
    title,
    ConvertDateToUnixtimeMillis(broadcast_start_datetime)    AS broadcast_start_timestamp,
    ConvertDateToUnixtimeMillis(broadcast_end_datetime)      AS broadcast_end_timestamp;

--集計用のチャンネルリストの作成
-- CROSS はバグがあるので JOIN で代用するために共通インデックスを付加
channel_list_00 = FOREACH contents_list GENERATE
    screen_name,
    channel_name,
    1               AS cross_key;
channel_list = DISTINCT channel_list_00;

-- L2) ユーザー属性情報
user_table = LOAD '/user/hadoop/summary/niconicodb/niconico_user_table/current' USING parquet.pig.ParquetLoader();
user_table = FOREACH user_table GENERATE
    user_id AS huid,
    sex,
    (birth_datetime IS NULL OR birth_datetime == '0000-00-00' OR SIZE(birth_datetime) != 10  ? '3000-01-01' : birth_datetime) AS birth_datetime,
    prefecture;
user_table = FOREACH user_table GENERATE
    huid,
    sex,
    ToDate(CONCAT(birth_datetime, ' 00:00:00'), 'yyyy-MM-dd HH:mm:ss') AS birth_datetime,
    prefecture;

-- L3) 動画情報をロードし、コンテンツリストと結合、およびスレッドID変換用の準備
video_table_00 = bblogload_content_nicovideo_video_v1('current');
video_table_10 = JOIN contents_list BY contents_id, video_table_00 BY contents_id;
video_table = FOREACH video_table_10 GENERATE
    contents_list::screen_name                  AS screen_name,
    contents_list::channel_name                 AS channel_name,
    contents_list::program_name                 AS program_name,
    contents_list::contents_id                  AS contents_id,
    contents_list::title                        AS title,
    contents_list::broadcast_start_timestamp    AS broadcast_start_timestamp,
    contents_list::broadcast_end_timestamp      AS broadcast_end_timestamp,
    FLATTEN(
        TOKENIZE(video_table_00::thread_ids)
    )                                           AS thread_id;

-- コンテンツ視聴ログ
contents_watch = bblogload_common_action_watch_event_v3('{$WATCH_SCREENMODE_LOAD_DAYS}');
contents_watch = FILTER contents_watch BY frontend_id == 6;
contents_watch = FILTER contents_watch BY event_type == 'start' OR event_type == 'end';
contents_watch = FOREACH contents_watch GENERATE
    user_id                                                            AS huid,
    content_id                                                         AS contents_id,
    getDeviceSystemType(ua, system_type)                               AS device,
    watch_milliseconds                                                 AS play_time_msec,
    ConvertDateToUnixtimeMillis(ToString(time, 'yyyy-MM-dd HH:mm:ss')) AS access_timestamp,
    ConvertDateToUnixtimeMillis(ToString(time, 'yyyy-MM-dd HH:mm:ss')) AS play_start_timestamp;

-- 再生時間が正数かつ再生日時が集計期間内のみを抽出
contents_watch = FILTER contents_watch BY
        play_time_msec >= 0
    AND
        $START_DATE_TIMESTAMP <= play_start_timestamp
    AND
        play_start_timestamp < $END_DATE_TIMESTAMP;

-- L5) 特集ページ集計用 apache ログ (channel_apache_pc)
-- CROSS はバグがあるので  INNER JOIN で代用するために共通キー(cross_key)を付加
channel_apache_log_00 = bblogload_specific_httpd_category_v2_using_mode_filter('nicochannel_web','{$WATCH_SCREENMODE_LOAD_DAYS}');
channel_apache_log_10 = FOREACH channel_apache_log_00 GENERATE
    user_id                               AS huid,
    ConvertDateToUnixtimeMillis(ToString(time, 'yyyy-MM-dd HH:mm:ss')) AS access_timestamp,
    time                                  AS access_datetime,
    status,
    SplitUri(uri, 'PATH')                 AS request_path,
    getDeviceSystemType(ua, system_type)  AS device,
    1                                     AS cross_key;

-- guest は除去、機種はPCのみ、リダイレクトされた物は除去、集計期間の内の再生に絞り込む
channel_apache_log = FILTER channel_apache_log_10 BY
        huid != 'guest'
    AND
        device == 'pc'
    AND
        status == 200
    AND
        $START_DATE_TIMESTAMP <= access_timestamp
    AND
        access_timestamp < $END_DATE_TIMESTAMP;


--------------------------------------------------------------------------------
-- 中間ログの生成 （仕様: https://atl.dwango.co.jp/confluence/x/jTPNAw）
-- M1) 再生ページ集計用ログ(watch_screen_mode_log)の成形処理
--      watch_screen_mode_logに動画情報を付与する。なお、スレッドID変換も行う
--      user情報を付加
-- M2) 特集ページ集計用ログ（channel_apache_log)の成形処理
-- M3) ２つを一つにまとめる。
--------------------------------------------------------------------------------
-- M1) 再生ページ集計用ログ(contents_watch) を中間ログ形式へ
-- スレッドIDとそれ以外に分割
SPLIT contents_watch INTO
    split_thread_id_log_00 IF contents_id MATCHES '^\\d*$',
    split_other_id_log_00 OTHERWISE;

-- スレッドIDで動画情報を付与
split_thread_id_log = match_watch_screen_and_video_attribute(split_thread_id_log_00, video_table, 'contents_id', 'thread_id');

-- コンテンツIDで動画情報を付与
split_other_id_log = match_watch_screen_and_video_attribute(split_other_id_log_00, contents_list, 'contents_id', 'contents_id');

content_middle_log_00 = UNION split_thread_id_log, split_other_id_log;

-- 各コンテンツの放送時間の範囲内にフィルタリング
content_middle_log_10 = FILTER content_middle_log_00 BY
        broadcast_start_timestamp <= play_start_timestamp
    AND
        play_start_timestamp < broadcast_end_timestamp;

-- ユーザー属性情報を紐付け
content_middle_log_20 = JOIN content_middle_log_10 BY huid LEFT OUTER, user_table BY huid;
content_middle_log_30 = FOREACH content_middle_log_20 GENERATE
    content_middle_log_10::huid                         AS huid,
    content_middle_log_10::access_timestamp             AS access_timestamp,
    content_middle_log_10::play_start_timestamp         AS play_start_timestamp,
    content_middle_log_10::play_time_msec               AS play_time_msec,
    content_middle_log_10::device                       AS device,
    content_middle_log_10::screen_name                  AS screen_name,
    content_middle_log_10::channel_name                 AS channel_name,
    content_middle_log_10::program_name                 AS program_name,
    content_middle_log_10::contents_id                  AS contents_id,
    content_middle_log_10::title                        AS title,
    user_table::sex                                     AS sex,
    user_table::birth_datetime                          AS birth_datetime,
    user_table::prefecture                              AS prefecture;

-- 中間ログ形式に成形
content_middle_log = FOREACH content_middle_log_30 GENERATE
    huid,
    ToDate(access_timestamp)                    AS access_datetime,
    ToDate(play_start_timestamp)                AS play_start_datetime,
    'content'                                   AS access_page_type,
    screen_name,
    channel_name,
    program_name,
    contents_id,
    title,
    device,
    (
        sex IS NULL
        ? 'null'
        : sex
    )                                           AS sex,
    (
        birth_datetime IS NULL
        ? 'null'
        : ConvertAgeToAgeGroup(
            YearsBetween(
                ToDate('$LAST_APRIL2ND_DATETIME'),
                birth_datetime
            ) + 1,
            'social_category'
        )
    )                                           AS social_category,
    (
        prefecture IS NULL
        ? 'null'
        : prefecture
    )                                           AS prefecture,
    (play_time_msec > 0 ? 1 : 0)                AS is_play,
    play_time_msec;

-- M2) 特集ページ集計用ログ（channel_apache_log)を中間ログ形式へ
channel_middle_log_00 = JOIN channel_apache_log BY cross_key, channel_list BY cross_key;
channel_middle_log_10 = FOREACH channel_middle_log_00 GENERATE
    channel_apache_log::huid                    AS huid,
    channel_apache_log::access_datetime         AS access_datetime,
    channel_apache_log::device                  AS device,
    channel_apache_log::request_path            AS request_path,
    channel_list::screen_name                   AS screen_name,
    channel_list::channel_name                  AS channel_name;

-- request_path が '/screen_name' である物を抽出
channel_middle_log_20 = FILTER channel_middle_log_10 BY INDEXOF(request_path, screen_name, 0) == 1;

-- ユーザー属性情報を紐付け
channel_middle_log_30 = JOIN channel_middle_log_20 BY huid, user_table BY huid;
channel_middle_log_40 = FOREACH channel_middle_log_30 GENERATE
    channel_middle_log_20::huid                 AS huid,
    channel_middle_log_20::access_datetime      AS access_datetime,
    channel_middle_log_20::screen_name          AS screen_name,
    channel_middle_log_20::channel_name         AS channel_name,
    channel_middle_log_20::device               AS device,
    user_table::sex                             AS sex,
    user_table::birth_datetime                  AS birth_datetime,
    user_table::prefecture                      AS prefecture;

-- 中間ログ形式に成形
channel_middle_log = FOREACH channel_middle_log_40 GENERATE
    huid,
    access_datetime,
    null                                        AS play_start_datetime,
    'channel'                                   AS access_page_type,
    screen_name,
    channel_name,
    null                                        AS program_name,
    null                                        AS contents_id,
    null                                        AS title,
    device,
    (
        sex IS NULL
        ? 'null'
        : sex
    )                                           AS sex,
    (
        birth_datetime IS NULL
        ? 'null'
        : ConvertAgeToAgeGroup(
            YearsBetween(
                ToDate('$LAST_APRIL2ND_DATETIME'),
                birth_datetime
            ) + 1,
            'social_category'
        )
    )                                           AS social_category,
    (
        prefecture IS NULL
        ? 'null'
        : prefecture
    )                                           AS prefecture,
    0                                           AS is_play,
    0                                           AS play_time_msec;

-- M3) 2種類のログを一つにまとめる
middle_log = UNION content_middle_log, channel_middle_log;

--------------------------------------------------------------------------------
-- レポートの出力
--------------------------------------------------------------------------------
rmf $OUTDIR01;
STORE middle_log INTO '$OUTDIR01' USING PigStorage('\t', '-schema');
