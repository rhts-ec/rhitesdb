# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0006_auto_20171111_1601'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='datavalue',
            name='category_str',
        ),
        migrations.AddField(
            model_name='datavalue',
            name='category_combo',
            field=models.ForeignKey(default=1, related_name='data_values', to='cannula.CategoryCombo'),
        ),
    ]
