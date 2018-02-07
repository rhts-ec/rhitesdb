# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0003_datavalue_org_unit'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='orgunit',
            options={'verbose_name': 'organisation unit'},
        ),
        migrations.AddField(
            model_name='dataelement',
            name='alias',
            field=models.CharField(null=True, blank=True, max_length=128),
        ),
        migrations.AddField(
            model_name='dataelement',
            name='dhis2_uid',
            field=models.CharField(null=True, blank=True, max_length=11),
        ),
        migrations.AlterUniqueTogether(
            name='orgunit',
            unique_together=set([('name', 'parent')]),
        ),
    ]
