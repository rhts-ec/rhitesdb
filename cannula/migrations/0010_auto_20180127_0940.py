# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0009_validationrule'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataelement',
            name='name',
            field=models.CharField(unique=True, max_length=128),
        ),
    ]
