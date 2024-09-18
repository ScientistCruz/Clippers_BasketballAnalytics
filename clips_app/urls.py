from django.urls import path
from . import views


urlpatterns = [
    path("team/", views.team_stats, name = 'team_stats'),
    path("league/", views.league_stats, name = 'league_stats'),
    # path("lineups/", views.lineups, name = 'lineups'),
    # path("lineups/", views.lineups, name = 'lineups'),



]