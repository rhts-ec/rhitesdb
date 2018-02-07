# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0010_auto_20180127_0940'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='datavalue',
            unique_together=set([('data_element', 'category_combo', 'org_unit', 'year', 'quarter', 'month')]),
        ),
    ]
