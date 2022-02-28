namespace Miruken.Quartz;

using Microsoft.Extensions.DependencyInjection;
using Register;

public static class RegistrationExtensions
{
    public static Registration WithQuartz(this Registration registration)
    {
        if (!registration.CanRegister(typeof(RegistrationExtensions)))
            return registration;

        return registration.Services(services =>
        {
            services.AddHostedService<JobRunnerHostedService>();
        });
    }
}