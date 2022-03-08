# Generated by Django 4.0.3 on 2022-03-07 19:53

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="User",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date_create", models.DateTimeField()),
                ("personal_info", models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name="EmailMessage",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date_sent", models.DateTimeField()),
                (
                    "type",
                    models.CharField(
                        choices=[
                            ("registration", "Registration"),
                            ("contract_created", "Contract Created"),
                            ("contract_validated", "Contract Validated"),
                        ],
                        max_length=100,
                    ),
                ),
                ("content", models.TextField()),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="core.user"
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="Contract",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date_create", models.DateTimeField()),
                ("date_validate", models.DateTimeField(null=True)),
                (
                    "state",
                    models.CharField(
                        choices=[("created", "Created"), ("validated", "Validated")],
                        max_length=100,
                    ),
                ),
                ("more_personal_info", models.TextField()),
                ("value", models.FloatField()),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="core.user"
                    ),
                ),
            ],
        ),
    ]
