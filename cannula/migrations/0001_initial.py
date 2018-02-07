# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.core.files.storage
import cannula.models


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='DataElement',
            fields=[
                ('id', models.AutoField(serialize=False, verbose_name='ID', primary_key=True, auto_created=True)),
                ('name', models.CharField(max_length=128)),
                ('value_type', models.CharField(choices=[('NUMBER', 'Number'), ('INTEGER', 'Integer (whole numbers only)'), ('POS_INT', 'Positive Integer')], max_length=8)),
                ('value_min', models.DecimalField(verbose_name='Minimum Value', max_digits=17, null=True, decimal_places=4, blank=True)),
                ('value_max', models.DecimalField(verbose_name='Maximum Value', max_digits=17, null=True, decimal_places=4, blank=True)),
                ('aggregation_method', models.CharField(choices=[('SUM', 'Sum()')], max_length=8)),
            ],
        ),
        migrations.CreateModel(
            name='DataValue',
            fields=[
                ('id', models.AutoField(serialize=False, verbose_name='ID', primary_key=True, auto_created=True)),
                ('category_str', models.CharField(max_length=128)),
                ('site_str', models.CharField(max_length=128)),
                ('numeric_value', models.DecimalField(max_digits=17, decimal_places=4)),
                ('month', models.CharField(null=True, max_length=7, blank=True)),
                ('quarter', models.CharField(null=True, max_length=7, blank=True)),
                ('year', models.CharField(null=True, max_length=4, blank=True)),
                ('data_element', models.ForeignKey(related_name='data_values', to='cannula.DataElement')),
            ],
        ),
        migrations.CreateModel(
            name='SourceDocument',
            fields=[
                ('id', models.AutoField(serialize=False, verbose_name='ID', primary_key=True, auto_created=True)),
                ('orig_filename', models.CharField(null=True, max_length=128, blank=True)),
                ('file', models.FileField(storage=django.core.files.storage.FileSystemStorage(location='/mnt/c/Users/ebichete/Documents/Assey-Muhereza/RHITES-EC/rhites_ec_web/source_doc_storage'), upload_to=cannula.models.make_random_filename)),
                ('uploaded_at', models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.AddField(
            model_name='datavalue',
            name='source_doc',
            field=models.ForeignKey(related_name='data_values', to='cannula.SourceDocument'),
        ),
    ]
