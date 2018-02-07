# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0002_orgunit'),
    ]

    operations = [
        migrations.AddField(
            model_name='datavalue',
            name='org_unit',
            field=models.ForeignKey(to='cannula.OrgUnit', default=1, related_name='data_values'),
            preserve_default=False,
        ),
    ]
