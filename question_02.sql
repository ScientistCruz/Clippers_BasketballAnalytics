-- Use lac_fullstack_dev
-- Go

-- CREATE VIEW question_02 AS

-- 2. Basic Queries (SQL)
-- a. Write a SQL query that can calculate team win-loss records, sorted by win percentage (defined as wins divided by games played)


WITH home_stats as (select a.*, b.teamName, ROW_NUMBER() OVER (ORDER BY W + L DESC)  AS home_games_rank 
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
                        on a.team_id = b.teamId),
        away_stats as (select a.*, b.teamName ,ROW_NUMBER() OVER (ORDER BY W + L DESC)  AS away_games_rank 
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
                        on a.team_id = b.teamId)

select a.teamName, wins + loses as games_played, wins, loses, win_percent as win_percentage ,
b.home_games_rank, c.away_games_rank, ROW_NUMBER() OVER (ORDER BY wins + loses DESC) as total_games_rank
from (select sum(W) as wins, sum(L) as loses, cast(round(cast(sum(W) as decimal(16,2))/(sum(L) + sum(W)), 2) as decimal (16,2)) as win_percent, team_id, teamName 
    from (select * from home_stats
        union all
        select * from away_stats) a
    group by team_id, teamName) a
left join (select teamName, home_games_rank from home_stats) b
on a.teamName = b.teamName
left join (select teamName, away_games_rank from away_stats) c
on a.teamName = c.teamName

order by win_percent desc





-- i. Final table should include team name, games played, wins, losses, win percentage
-- b. In the same table, show how the team ranks (highest to lowest) in terms of games played, home games, and away games during this month of the season? Make sure your code can extend to additional months as data is added to the data set. For each, show both the number of games and the rank