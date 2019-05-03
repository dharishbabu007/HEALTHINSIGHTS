import { ResetPasswordModule } from './reset-password.module';

describe('SignupModule', () => {
  let resetPasswordModule: ResetPasswordModule;

  beforeEach(() => {
    resetPasswordModule = new ResetPasswordModule();
  });

  it('should create an instance', () => {
    expect(resetPasswordModule).toBeTruthy();
  });
});
