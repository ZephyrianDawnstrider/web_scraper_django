from django.urls import path
from . import views

app_name = 'scraper'

urlpatterns = [
    path('', views.home, name='home'),
    path('site-mapping/', views.site_mapping, name='site_mapping'),
    path('web-crawling/', views.web_crawling, name='web_crawling'),
    path('download/', views.download, name='download'),
    path('view/', views.view_data, name='view_data'),
    path('view/<slug:url_slug>/', views.url_data, name='url_data'),
    path('scrape_progress/', views.get_scrape_progress, name='scrape_progress'),
]
