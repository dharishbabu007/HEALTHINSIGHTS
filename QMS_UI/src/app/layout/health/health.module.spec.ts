import { HealthModule } from './health.module';

describe('HealthModule', () => {
  let healthModule: HealthModule;

  beforeEach(() => {
    healthModule = new HealthModule();
  });

  it('should create an instance', () => {
    expect(healthModule).toBeTruthy();
  });
});
