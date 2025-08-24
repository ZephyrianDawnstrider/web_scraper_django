from django.db import migrations, models
import django.db.models.deletion

class Migration(migrations.Migration):

    dependencies = [
        ('scraper', '0001_initial'),
    ]

    operations = [
        # Add additional fields to existing models
        migrations.AddField(
            model_name='mainurl',
            name='status',
            field=models.CharField(max_length=20, default='pending'),
        ),
        migrations.AddField(
            model_name='mainurl',
            name='error_message',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='suburl',
            name='status_code',
            field=models.IntegerField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='suburl',
            name='scraped_at',
            field=models.DateTimeField(null=True, blank=True),
        ),
        
        # Create new model for scraping sessions
        migrations.CreateModel(
            name='ScrapingSession',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('session_name', models.CharField(max_length=255)),
                ('start_time', models.DateTimeField(auto_now_add=True)),
                ('end_time', models.DateTimeField(null=True, blank=True)),
                ('total_urls', models.IntegerField(default=0)),
                ('successful_urls', models.IntegerField(default=0)),
                ('failed_urls', models.IntegerField(default=0)),
                ('settings', models.JSONField(default=dict)),
            ],
            options={
                'db_table': 'scraping_session',
            },
        ),
        
        # Add foreign key to MainURL
        migrations.AddField(
            model_name='mainurl',
            name='session',
            field=models.ForeignKey(null=True, blank=True, on_delete=django.db.models.deletion.CASCADE, to='scraper.ScrapingSession'),
        ),
    ]
