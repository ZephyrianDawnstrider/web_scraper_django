from django.urls import path
from . import views

app_name = 'scraper'

urlpatterns = [
    path('', views.index, name='index'),
    path('view/', views.view_data, name='view_data'),
    path('download/', views.download, name='download'),
    path('view/<slug:url_slug>/', views.url_data, name='url_data'),
    path('scrape_progress/', views.get_scrape_progress, name='scrape_progress'),
]
