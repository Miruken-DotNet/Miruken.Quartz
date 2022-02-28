namespace Miruken.Quartz;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Api;
using Callback;
using Callback.Policy;
using CronExpressionDescriptor;
using global::Quartz;
using global::Quartz.Impl;
using global::Quartz.Spi;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class JobRunnerHostedService : BackgroundService
{
    private readonly ILogger _logger;
    private readonly IHandler _composer;
    private readonly IConfigurationSection _jobs;
    private readonly string _defaultGroup;
    private IScheduler _scheduler;

    public JobRunnerHostedService(
        IConfiguration configuration,
        ILogger        logger,
        IHandler       composer)
    {
        _logger        = logger;
        _composer      = composer;

        _jobs          = configuration.GetSection("Jobs");
        _defaultGroup  = _jobs["DefaultGroup"];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _scheduler = await CreateScheduler(stoppingToken);
        await _scheduler.Start(stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _scheduler.Shutdown(cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    protected virtual IHandler PrepareJobEnvironment() => _composer;

    private async Task<IScheduler> CreateScheduler(CancellationToken stoppingToken)
    {
        var factory   = new StdSchedulerFactory();
        var scheduler = await factory.GetScheduler(stoppingToken);
        scheduler.JobFactory = new JobAdapter(PrepareJobEnvironment());

        foreach (var methodBinding in Handles.Policy.GetMethods())
        {
            if (methodBinding.Dispatcher is GenericMethodDispatch)
                continue;

            var schedule = methodBinding.Dispatcher
                .Attributes.OfType<ScheduleAttribute>()
                .FirstOrDefault();

            if (schedule == null) continue;
            if (methodBinding.Key is Type jobType)
            {
                if (jobType.GetConstructor(Type.EmptyTypes) == null)
                {
                    throw new InvalidOperationException(
                        $"Job '{jobType}' requires a default constructor to run");
                }

                ScheduleJob(jobType, schedule, scheduler);
                continue;
            }

            var member = methodBinding.Dispatcher.Member;
            throw new InvalidOperationException(
                $"Unrecognized scheduled job '{methodBinding.Key}' for {member}");
        }

        return scheduler;
    }

    private void ScheduleJob(Type jobType, ScheduleAttribute schedule, IScheduler scheduler)
    {
        var logicalJobType = jobType;
        if (logicalJobType.IsGenericType)
        {
            var jobDefinition = logicalJobType.GetGenericTypeDefinition();
            if (jobDefinition == typeof(Trigger<>))
                logicalJobType = logicalJobType.GetGenericArguments()[0];
        }

        var schedulerKey = schedule.ScheduleKey ?? logicalJobType.FullName ?? "";
        var jobSchedule  = _jobs[schedulerKey];

        if (string.IsNullOrEmpty(jobSchedule))
        {
            jobSchedule = _jobs[logicalJobType.Name];
        }

        while (string.IsNullOrEmpty(jobSchedule))
        {
            var dot = schedulerKey.LastIndexOf('.');
            if (dot <= 0) break;
            schedulerKey = schedulerKey.Substring(0, dot);
            jobSchedule  = _jobs[schedulerKey];
        }

        if (string.IsNullOrEmpty(jobSchedule))
        {
            throw new InvalidOperationException(
                $"Could not determine schedule for Job '{logicalJobType}'.  Did you forget to define it in the Jobs section of appSettings.");
        }

        var description = ExpressionDescriptor.GetDescription(jobSchedule);
        _logger.LogInformation($"Scheduling job '{logicalJobType}' to run {description}");

        var jobBuilder = JobBuilder.Create<JobAdapter>();
        var jobGroup   = schedule.Group ?? _defaultGroup;

        var job = (string.IsNullOrEmpty(jobGroup)
                ? jobBuilder.WithIdentity(logicalJobType.FullName!)
                : jobBuilder.WithIdentity(logicalJobType.FullName!, jobGroup))
            .Build();

        job.JobDataMap.Put("Request", jobType);

        var trigger = TriggerBuilder.Create()
            .StartNow()
            .WithCronSchedule(jobSchedule)
            .Build();

        scheduler.ScheduleJob(job, trigger);
    }

    [DisallowConcurrentExecution]
    private class JobAdapter : IJobFactory, IJob
    {
        private readonly IHandler _handler;

        public JobAdapter(IHandler handler) => _handler = handler;

        public Task Execute(IJobExecutionContext context)
        {
            var request = (Type)context.JobDetail.JobDataMap["Request"];
            return _handler
                .With(context.CancellationToken)
                .Send(Activator.CreateInstance(request));
        }

        public IJob NewJob(TriggerFiredBundle bundle, IScheduler scheduler) => this;

        public void ReturnJob(IJob job) { }
    }
}