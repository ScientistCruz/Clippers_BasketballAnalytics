import pyodbc
import sqlalchemy
import pandas as pd
import os
import pathlib
import sys
import pandas as pd

dname = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join(dname,'utilities')
dev_test_data = os.path.join(dname,'dev_test_data')

sys.path.insert(0, utils)

import utilities
from utilities.utils import sql_server

# Question #1: Database creation (Python, SQL, other scripting languages)

if __name__ == '__main__':
    connection = sql_server(server = 'sql_server' , username = 'sa', port = 1443, password = 'tEST1234')
    print(connection.connection)

    connection.create_database(database='lac_fullstack_dev')
    connection.create_database(database='lac_fullstack_dev_staging')

    # datetimes
    onlyfiles = [f for f in os.listdir(dev_test_data) if os.path.isfile(os.path.join(dev_test_data, f))]
    print(onlyfiles)

    for file in os.listdir(dev_test_data):

        if '.json' in file:
            file_df = pd.read_json(os.path.join(dev_test_data, file))
            table_name = file.replace('.json', '')
        else:
            pass

        if table_name == 'player':
            merge_column = ['player_id']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')
        elif table_name == 'roster':
            merge_column = ['team_id', 'player_id']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')
            file_df[merge_column[1]] = file_df[[merge_column[1]]].astype('int64')

        elif table_name == 'game_schedule':
            merge_column = ['game_id']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')
            file_df['home_score'] = file_df['home_score'].astype('int64')
            file_df['home_id'] = file_df['home_id'].astype('int64')
            file_df['away_score'] = file_df['away_score'].astype('int64')
            file_df['away_id'] = file_df['away_id'].astype('int64')            

        elif table_name == 'team_affiliate':
            merge_column = ['nba_teamId']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')

            file_df['glg_teamId'] = file_df['glg_teamId'].fillna(0)

            file_df['glg_teamId'] = file_df['glg_teamId'].astype('string')
            file_df['glg_teamId'] = file_df['glg_teamId'].str.replace('.0', '')

            file_df['glg_teamId'] = file_df['glg_teamId'].astype('int64')

        elif table_name == 'lineup':
            # merge_column = ['team_id', 'player_id', 'lineup_num']
            merge_column = ['team_id', 'lineup_num', 'game_id', 'player_id']
            file_df = file_df.astype('int64')
        elif table_name == 'team':
            merge_column = ['teamId']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')

        else:
            print('Table [merge column] needs to mapped.')


        connection.sql_query_bt(file_df,table_name, merge_column, 0)        




# ------------------------------------------------------------------------------------------------------------------------------------
    # Question #2

    home_stats_df = """select a.*, b.teamName, ROW_NUMBER() OVER (ORDER BY W + L DESC)  AS home_games_rank 
                    from (select sum(HOME_WIN) as W, sum(HOME_LOSE) as L,  home_id as team_id
                        from (select game_id, home_id, home_score, away_id, away_score,
                                case when home_score > away_score then 1
                                else 0
                                END 
                                as HOME_WIN,
                                case when home_score < away_score then 1
                                else 0
                                END 
                                as HOME_LOSE        


                                from lac_fullstack_dev.dbo.game_schedule a
                                -- where home_id = '1610612743'
                                )a 
                        group by home_id) a

                        left join lac_fullstack_dev.dbo.team b
                        on a.team_id = b.teamId"""
    home_stats_df = connection.execute_query_return_df(home_stats_df, 'lac_fullstack_dev')
    connection.sql_query_bt(home_stats_df,'q2_home_stats', ['team_id'], 0)    


    away_stats_df = """select a.*, b.teamName ,ROW_NUMBER() OVER (ORDER BY W + L DESC)  AS away_games_rank 
                        from (select sum(away_WIN) as W, sum(away_LOSE) as L,  away_id as team_id
                            from (select game_id, home_id, home_score, away_id, away_score,
                                    case when home_score < away_score then 1
                                    else 0
                                    END 
                                    as away_WIN,
                                    case when home_score > away_score then 1
                                    else 0
                                    END 
                                    as away_LOSE        


                                    from lac_fullstack_dev.dbo.game_schedule a
                                    -- where away_id = '1610612743'
                                    )a 
                            group by away_id) a

                        left join lac_fullstack_dev.dbo.team b
                        on a.team_id = b.teamId"""

    away_stats_df = connection.execute_query_return_df(away_stats_df, 'lac_fullstack_dev')
    connection.sql_query_bt(away_stats_df,'q2_away_stats', ['team_id'], 0)    

    question_02 = """select a.teamName, wins + losses as games_played, wins, losses, win_percent as win_percentage ,
            b.home_games_rank, c.away_games_rank, ROW_NUMBER() OVER (ORDER BY wins + losses DESC) as total_games_rank
            from (select sum(W) as wins, sum(L) as losses, cast(round(cast(sum(W) as decimal(16,2))/(sum(L) + sum(W)), 2) as decimal (16,2)) as win_percent, team_id, teamName 
                from (select * from q2_home_stats
                    union all
                    select * from q2_away_stats) a
                group by team_id, teamName) a
            left join (select teamName, home_games_rank from q2_home_stats) b
            on a.teamName = b.teamName
            left join (select teamName, away_games_rank from q2_away_stats) c
            on a.teamName = c.teamName

            order by win_percent desc"""

    question_02 = connection.execute_query_return_df(question_02, 'lac_fullstack_dev')
    connection.sql_query_bt(question_02,'q2_final', ['teamName'], 0) 


# ------------------------------------------------------------------------------------------------------------------------------------

    # Question #3A


    home_BBs_ro = """select home_id as team_id, game_date, ROW_NUMBER() OVER(ORDER BY home_id, game_date) as rn 
                    from lac_fullstack_dev.dbo.game_schedule """
    home_BBs_ro = connection.execute_query_return_df(home_BBs_ro, 'lac_fullstack_dev')
    connection.sql_query_bt(home_BBs_ro,'q3_a_home_BBs_ro', ['team_id', 'game_date'], 0)    


    away_BBs_ro = """select away_id as team_id, game_date, ROW_NUMBER() OVER(ORDER BY away_id, game_date) as rn 
                    from lac_fullstack_dev.dbo.game_schedule """
    away_BBs_ro = connection.execute_query_return_df(away_BBs_ro, 'lac_fullstack_dev')
    connection.sql_query_bt(away_BBs_ro,'q3_a_away_BBs_ro', ['team_id', 'game_date'], 0) 

    home_BBs = """select a.team_id, b.game_date as game_date_g1_bb, a.game_date as game_date_g2_bb from q3_a_home_BBs_ro a
                left join q3_a_home_BBs_ro b
                on a.rn = b.rn + 1 and a.team_id = b.team_id and DATEDIFF(day, b.game_date,a.game_date) = 1
                where b.team_id is not null"""
    home_BBs = connection.execute_query_return_df(home_BBs, 'lac_fullstack_dev')
    connection.sql_query_bt(home_BBs,'q3_a_home_BBs', ['team_id','game_date_g1_bb'], 0) 


    away_BBs = """select a.team_id, b.game_date as game_date_g1_bb, a.game_date as game_date_g2_bb from q3_a_away_BBs_ro a
                left join q3_a_away_BBs_ro b
                on a.rn = b.rn + 1 and a.team_id = b.team_id and DATEDIFF(day, b.game_date,a.game_date) = 1
                where b.team_id is not null"""
    away_BBs = connection.execute_query_return_df(away_BBs, 'lac_fullstack_dev')
    connection.sql_query_bt(away_BBs,'q3_a_away_BBs', ['team_id', 'game_date_g1_bb'], 0) 


    # all_BBs = """select * 
    #             from(select b.teamName, a.* , 'home' as bb_type from q2_home_BBs a
    #                 left join lac_fullstack_dev.dbo.team b
    #                 on a.team_id = b.teamId

    #                 union all

    #                 select b.teamName, a.*, 'away' as bb_type from q2_away_BBs a
    #                 left join lac_fullstack_dev.dbo.team b
    #                 on a.team_id = b.teamId) a
    #             order by game_date_g1_bb asc
    #             """
    # all_BBs = connection.execute_query_return_df(all_BBs, 'lac_fullstack_dev')
    # connection.sql_query_bt(all_BBs,'q2_all_BBs', ['team_id'], 0) 

    question_03_a = """select * , 'home' as bb_type from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'home' as bb_type from q3_a_home_BBs a
                    left join lac_fullstack_dev.dbo.team b
                    on a.team_id = b.teamId) a
                    group by teamName) a

            where total_home_BBs = (select max(total_home_BBs) from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'home' as bb_type from q3_a_home_BBs a
                                    left join lac_fullstack_dev.dbo.team b
                                    on a.team_id = b.teamId) a
                                    group by teamName) a)

            union all
            -- Which had the most Away-Away B2Bs? 
            select *, 'away' as bb_type from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'away' as bb_type from q3_a_away_BBs a
                    left join lac_fullstack_dev.dbo.team b
                    on a.team_id = b.teamId) a
                    group by teamName) a

            where total_home_BBs = (select max(total_home_BBs) from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'away' as bb_type from q3_a_away_BBs a
                                    left join lac_fullstack_dev.dbo.team b
                                    on a.team_id = b.teamId) a
                                    group by teamName) a)
                """
    question_03_a = connection.execute_query_return_df(question_03_a, 'lac_fullstack_dev')
    connection.sql_query_bt(question_03_a,'q3_a_final', ['teamName', 'bb_type'], 0) 

    #Question #3B

    games_ro = """select *, ROW_NUMBER() OVER(PARTITION by team_id ORDER BY team_id, game_date) as rn from (select home_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule 
                UNION
                select away_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule ) a"""
    games_ro = connection.execute_query_return_df(games_ro, 'lac_fullstack_dev')
    connection.sql_query_bt(games_ro,'q3_b_games_ro', ['team_id', 'game_date'], 0) 

    games = """select a.team_id, b.game_date as game_date_g1, a.game_date as game_date_g2, datediff(day, cast(b.game_date as datetime), cast(a.game_date as datetime)) as rest_days from q3_b_games_ro a
                left join q3_b_games_ro b
                on a.rn = b.rn + 1 and a.team_id = b.team_id """
    games = connection.execute_query_return_df(games, 'lac_fullstack_dev')
    connection.sql_query_bt(games,'q3_b_games', ['team_id', 'game_date_g1'], 0)  

    question_03_b = """select b.teamName, a.* from q3_b_games a
                left join lac_fullstack_dev.dbo.team b
                        on a.team_id = b.teamId
                where rest_days = (select max(rest_days) from q3_b_games)"""
    question_03_b = connection.execute_query_return_df(question_03_b, 'lac_fullstack_dev')
    connection.sql_query_bt(question_03_b,'q3_b_final', ['team_id'], 0) 

    question_03_c = """select count(*) as three_in_four, b.teamName 
                    from (select count(*) as total_games, team_id, og_gd
                            from (select a.team_id , a.game_date as og_gd, b.game_date from q3_b_games_ro a
                            left join q3_b_games_ro b
                            on a.team_id = b.team_id and abs(DATEDIFF(day,a.game_date,b.game_date)) <= 2) a
                            -- where team_id = '1610612747'
                            group by team_id, og_gd
                            ) a
                    left join lac_fullstack_dev.dbo.team b
                    on a.team_id = b.teamId        
                    group by b.teamName
                    order by count(*) desc"""
    question_03_c = connection.execute_query_return_df(question_03_c, 'lac_fullstack_dev')
    connection.sql_query_bt(question_03_c,'q3_c_final', ['teamName'], 0) 


# ------------------------------------------------------------------------------------------------------------------------------------

    # Question #4a


    question_04_a = """select game_id, team_id, lineup_num,[1] as player_1, [2] as player_2, [3] as player_3, [4] as player_4, [5] as player_5 
                    from (select team_id, game_id, player_id, lineup_num, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, lineup_num ORDER BY player_id) AS rn from lac_fullstack_dev.dbo.lineup
                    -- where team_id = '1610612747' and game_id = '66' and lineup_num = 5
                    ) a
                    PIVOT(max(player_id) 
                    for rn in ([1], [2], [3], [4], [5])) as pt"""
    question_04_a = connection.execute_query_return_df(question_04_a, 'lac_fullstack_dev')
    connection.sql_query_bt(question_04_a,'q4_a_final', ['game_id', 'team_id', 'lineup_num'], 0) 


    # Question #4b

    player_stints = """select a.game_id, b.game_date, c.teamName as team, d.teamName as opponent,
                    concat(e.first_name, ' ' , e.last_name) as player_name, 
                    period, period as stint_number, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as stint_start_time, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as stint_end_time

                    from (select game_id, team_id, player_id, period,  max(time_in) as time_in, min(time_out) as time_out 
                        from (select a.* 
                            from (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) a
                            left join (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) b
                            on a.game_id = b.game_id and a.team_id =  b.team_id and a.player_id = b.player_id and a.[period] = b.[period] and a.lineup_num = b.lineup_num - 1
                            and a.time_out = b.time_in) a
                        group by game_id, team_id, player_id, period) a
                    -- group by game_id, team_id, player_id, period) a
                    left join lac_fullstack_dev.dbo.game_schedule b
                    on a.game_id = b.game_id and a.team_id = b.home_id
                    left join lac_fullstack_dev.dbo.team c
                    on a.team_id = c.teamId
                    left join lac_fullstack_dev.dbo.team d
                    on b.away_id = d.teamId
                    left join lac_fullstack_dev.dbo.player e
                    on a.player_id = e.player_id
                    where b.game_date is not null
                    
                    union all

                    --  away_games
                    select a.game_id, b.game_date, c.teamName as team, d.teamName as opponent,
                    concat(e.first_name, ' ' , e.last_name) as player_name, 
                    period, period as stint_number, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as stint_start_time, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as stint_end_time

                    from (select game_id, team_id, player_id, period,  max(time_in) as time_in, min(time_out) as time_out 
                        from (select a.* 
                            from (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) a
                            left join (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) b
                            on a.game_id = b.game_id and a.team_id =  b.team_id and a.player_id = b.player_id and a.[period] = b.[period] and a.lineup_num = b.lineup_num - 1
                            and a.time_out = b.time_in) a
                        group by game_id, team_id, player_id, period) a
                    -- group by game_id, team_id, player_id, period) a
                    left join lac_fullstack_dev.dbo.game_schedule b
                    on a.game_id = b.game_id and a.team_id = b.away_id
                    left join lac_fullstack_dev.dbo.team c
                    on a.team_id = c.teamId
                    left join lac_fullstack_dev.dbo.team d
                    on b.away_id = d.teamId
                    left join lac_fullstack_dev.dbo.player e
                    on a.player_id = e.player_id
                    where b.game_date is not null"""
    
    player_stints = connection.execute_query_return_df(player_stints, 'lac_fullstack_dev')
    connection.sql_query_bt(player_stints,'q4_b_player_stints', ['game_id', 'team', 'player_name', 'stint_number'], 0) 



    question_04_b = """select * from q4_b_player_stints
                    where stint_start_time = '12:00' and stint_end_time = '00:00'"""
    question_04_b = connection.execute_query_return_df(question_04_b, 'lac_fullstack_dev')
    connection.sql_query_bt(question_04_b,'q4_b_final', ['game_id', 'team', 'player_name', 'stint_number'], 0) 


    question_04_c = """select b.game_id, b.team, a.player_name, a.avg_stints_per_game, b.avg_stint_length from (select avg(total_game_stints) as avg_stints_per_game, player_name from (select count(*) as total_game_stints , game_id, team, player_name  from q4_b_player_stints
                        group by game_id, team, player_name) a
                        group by player_name) a
                        left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,stint_end_time,stint_start_time)),0),108),5) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints
                        group by game_id, team, player_name) b
                        on a.player_name = b.player_name
                        -- where game_id = '102' and team = 'Milwaukee Bucks'
                        
                        """
    question_04_c = connection.execute_query_return_df(question_04_c, 'lac_fullstack_dev')
    connection.sql_query_bt(question_04_c,'q4_c_final', ['game_id', 'team', 'player_name'], 0) 

    game_stats = """select game_id, home_id, home_score, away_id, away_score,
                                case when home_score > away_score then 1
                                else 0
                                END 
                                as HOME_WIN,
                                case when home_score < away_score then 1
                                else 0
                                END 
                                as AWAY_WIN        


                                from lac_fullstack_dev.dbo.game_schedule a"""
    game_stats = connection.execute_query_return_df(game_stats, 'lac_fullstack_dev')
    connection.sql_query_bt(game_stats,'q4_d_game_stats', ['game_id'], 0)

    question_04_d_all = """select player_name, team, count(game_id) as total_games, avg(avg_stints_per_game) as avg_stints_per_game, RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(avg_stint_length),0),108),5)as avg_stint_length 
                        from (select b.game_id, b.team, c.teamId, a.player_name, a.avg_stints_per_game, b.avg_stint_length,
                        case when (c.teamid = d.home_id) and HOME_WIN = 1 then 1
                        when (c.teamid = d.away_id) and HOME_WIN = 1 then 1
                        else 0
                        end 
                        as win,
                        case when (c.teamid = d.home_id) and HOME_WIN = 0 then 1
                        when (c.teamid = d.away_id) and HOME_WIN = 0 then 1
                        else 0
                        end 
                        as lose
                        -- b.*,
                        -- d.*  
                        from (select avg(total_game_stints) as avg_stints_per_game, player_name 
                            from (select count(*) as total_game_stints , game_id, team, player_name  
                                from q4_b_player_stints
                                group by game_id, team, player_name) a
                            group by player_name) a
                        -- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,time_out,time_in)),0),108),5) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints
                        left join (select avg(datediff(minute,stint_end_time,stint_start_time)) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints

                        group by game_id, team, player_name) b
                        on a.player_name = b.player_name
                        left join lac_fullstack_dev.dbo.team c
                        on b.team = c.teamName
                        left join (select * from q4_d_game_stats) d
                        on b.game_id = d.game_id) a
                        -- where win = 1
                        group by player_name, team
                        
                        """
    question_04_d_all = connection.execute_query_return_df(question_04_d_all, 'lac_fullstack_dev')
    connection.sql_query_bt(question_04_d_all,'q4_d_final_all', ['player_name', 'team'], 0) 



    question_04_d_wins = """select player_name, team, count(game_id) as total_games, avg(avg_stints_per_game) as avg_stints_per_game, RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(avg_stint_length),0),108),5)as avg_stint_length 
                        from (select b.game_id, b.team, c.teamId, a.player_name, a.avg_stints_per_game, b.avg_stint_length,
                        case when (c.teamid = d.home_id) and HOME_WIN = 1 then 1
                        when (c.teamid = d.away_id) and HOME_WIN = 1 then 1
                        else 0
                        end 
                        as win,
                        case when (c.teamid = d.home_id) and HOME_WIN = 0 then 1
                        when (c.teamid = d.away_id) and HOME_WIN = 0 then 1
                        else 0
                        end 
                        as lose
                        -- b.*,
                        -- d.*  
                        from (select avg(total_game_stints) as avg_stints_per_game, player_name 
                            from (select count(*) as total_game_stints , game_id, team, player_name  
                                from q4_b_player_stints
                                group by game_id, team, player_name) a
                            group by player_name) a
                        -- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,time_out,time_in)),0),108),5) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints
                        left join (select avg(datediff(minute,stint_end_time,stint_start_time)) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints

                        group by game_id, team, player_name) b
                        on a.player_name = b.player_name
                        left join lac_fullstack_dev.dbo.team c
                        on b.team = c.teamName
                        left join (select * from q4_d_game_stats) d
                        on b.game_id = d.game_id) a
                        where win = 1
                        group by player_name, team
                        
                        """
    question_04_d_wins = connection.execute_query_return_df(question_04_d_wins, 'lac_fullstack_dev')
    connection.sql_query_bt(question_04_d_wins,'question_04_d_wins', ['player_name', 'team'], 0) 



    question_04_d_losses = """select player_name, team, count(game_id) as total_games, avg(avg_stints_per_game) as avg_stints_per_game, RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(avg_stint_length),0),108),5)as avg_stint_length 
                        from (select b.game_id, b.team, c.teamId, a.player_name, a.avg_stints_per_game, b.avg_stint_length,
                        case when (c.teamid = d.home_id) and HOME_WIN = 1 then 1
                        when (c.teamid = d.away_id) and HOME_WIN = 1 then 1
                        else 0
                        end 
                        as win,
                        case when (c.teamid = d.home_id) and HOME_WIN = 0 then 1
                        when (c.teamid = d.away_id) and HOME_WIN = 0 then 1
                        else 0
                        end 
                        as lose
                        -- b.*,
                        -- d.*  
                        from (select avg(total_game_stints) as avg_stints_per_game, player_name 
                            from (select count(*) as total_game_stints , game_id, team, player_name  
                                from q4_b_player_stints
                                group by game_id, team, player_name) a
                            group by player_name) a
                        -- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,time_out,time_in)),0),108),5) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints
                        left join (select avg(datediff(minute,stint_end_time,stint_start_time)) as avg_stint_length, game_id, team, player_name  from q4_b_player_stints

                        group by game_id, team, player_name) b
                        on a.player_name = b.player_name
                        left join lac_fullstack_dev.dbo.team c
                        on b.team = c.teamName
                        left join (select * from q4_d_game_stats) d
                        on b.game_id = d.game_id) a
                        where win = 0
                        group by player_name, team
                        
                        """
    question_04_d_losses = connection.execute_query_return_df(question_04_d_losses, 'lac_fullstack_dev')
    connection.sql_query_bt(question_04_d_losses,'question_04_d_losses', ['player_name', 'team'], 0) 