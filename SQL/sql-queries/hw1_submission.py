# Collaborators: Fill in names and UNIs here

def query_one():
    """Query for Columbia's venue"""
    return """
    SELECT DISTINCT(venue_name), venue_capacity 
    FROM `bigquery-public-data.ncaa_basketball.mbb_games_sr` 
    WHERE venue_id = (SELECT venue_id FROM `bigquery-public-data.ncaa_basketball.mbb_teams` WHERE market="Columbia") AND season=2013
    """

def query_two():
    """Query for games in Columbia's venue"""
    return """
  	SELECT COUNT(DISTINCT(game_id)) as num FROM `bigquery-public-data.ncaa_basketball.mbb_teams_games_sr` 
  	WHERE venue_id = (SELECT venue_id FROM `bigquery-public-data.ncaa_basketball.mbb_teams` WHERE market="Columbia") AND season = 2013
  """

def query_three():
    """Query for maximum-red-intensity teams"""
    return """      
    SELECT DISTINCT(home_market) as h_market, attendance, season FROM `bigquery-public-data.ncaa_basketball.mbb_pbp_sr` 
    WHERE season>=2013 AND season<=2019 
    ORDER BY attendance DESC 
    LIMIT 3 
   """

def query_four():
    """Query for Columbia's wins at home"""
    return """
  	SELECT COUNT(win) as number, ROUND(AVG(points_game),2) as avg_columbia, ROUND(AVG(opp_points_game),2) as avg_opponent 
  	FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_games` 
  	WHERE market="Columbia University-Barnard College" AND win=true AND season BETWEEN 2009 AND 2017 
  	GROUP BY market
  """

def query_five():
    """Query for players for birth state"""
    return """
    SELECT COUNT(DISTINCT(player_games_sr.player_id)) as num_players 
    FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr` AS player_games_sr INNER JOIN `bigquery-public-data.ncaa_basketball.mbb_teams` as mbb_teams ON player_games_sr.team_id=mbb_teams.id  
    WHERE player_games_sr.birthplace_state = mbb_teams.venue_state
  """

def query_six():
    """Query for biggest total points"""
    return """
    SELECT win_name, lose_name, win_pts, lose_pts, win_pts + lose_pts as total_points 
    FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games` 
    ORDER BY total_points DESC 
    LIMIT 1
  """

def query_seven():
    """Query for historical upset percentage"""
    return """
    SELECT ROUND(100 * COUNT(win_seed)/(SELECT COUNT(*) FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`),2) as upset_percentage 
    FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games` 
    WHERE lose_seed < win_seed AND win_pts>60
 """

def query_eight():
    """Query for teams with same states and first letter"""
    return """
    SELECT team_A.name as teamA, team_B.name as teamB, team_A.venue_state as state 
    FROM `bigquery-public-data.ncaa_basketball.mbb_teams` as team_A, `bigquery-public-data.ncaa_basketball.mbb_teams` as team_B 
    WHERE team_A.id <> team_B.id AND team_A.venue_state = team_B.venue_state AND SUBSTR(team_A.name,1,1) = SUBSTR(team_B.name,1,1) AND team_A.name < team_B.name 
    ORDER BY team_A.name ASC 
    LIMIT 3
    """

def query_nine():
    """Query for top geographical locations"""
    return """
    SELECT birthplace_city, birthplace_state, birthplace_country, sum(points) as total_points 
    FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr` 
    WHERE team_market = "Columbia" AND season BETWEEN 2013 AND 2016 AND points IS NOT NULL GROUP BY birthplace_city, birthplace_state, birthplace_country 
    ORDER BY sum(points) DESC, birthplace_city ASC 
    LIMIT 3
  """

def query_ten():
    """Query for teams with lots of high-scorers"""
    return """
    SELECT team_market, count(distinct player_id) as num_players FROM
	(SELECT game_id, team_market , player_id, sum(points_scored) FROM `bigquery-public-data.ncaa_basketball.mbb_pbp_sr` 
	WHERE period = 2 
	GROUP BY game_id,team_market, player_id HAVING SUM(points_scored)>=15) as pbp
	GROUP BY team_market
	ORDER BY num_players desc, team_market ASC
	LIMIT 5
  """

def query_eleven():
    """Query for highest-winner teams"""
    return """
  	SELECT a.market, COUNT(*) as bottom_performer_count 
  	FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons` as a, (SELECT season, max(losses) as loss FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons` WHERE season BETWEEN 1900 AND 2000 GROUP BY season) as b 
	WHERE a.season = b.season AND a.losses = b.loss AND a.market is not NULL GROUP BY a.market 
	ORDER BY COUNT(*) DESC, a.market ASC LIMIT 5

    """
    
    
