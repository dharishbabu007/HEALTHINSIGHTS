export * from './modules';
export * from './pipes/shared-pipes.module';
export * from './guard';

export function Replace(el: any): any {
    const nativeElement: HTMLElement = el.nativeElement;
    const parentElement: HTMLElement = nativeElement.parentElement;
    // move all children out of the element
    while (nativeElement.firstChild) {
      parentElement.insertBefore(nativeElement.firstChild, nativeElement);
    }
    // remove the empty element(the host)
    parentElement.removeChild(nativeElement);
  }

  const RemoveClasses = (NewClassNames) => {
    const MatchClasses = NewClassNames.map((Class) => document.querySelector('body').classList.contains(Class));
    return MatchClasses.indexOf(true) !== -1;
  };
  export const ToggleClasses = (Toggle, ClassNames) => {
    const Level = ClassNames.indexOf(Toggle);
    const NewClassNames = ClassNames.slice(0, Level + 1);
    if (RemoveClasses(NewClassNames)) {
      NewClassNames.map((Class) => document.querySelector('body').classList.remove(Class));
    } else {
      document.querySelector('body').classList.add(Toggle);
    }
  };
  export const sidebarCssClasses: Array<string> = [
    'sidebar-show',
    'sidebar-sm-show',
    'sidebar-md-show',
    'sidebar-lg-show',
    'sidebar-xl-show'
  ];
  export const asideMenuCssClasses: Array<string> = [
    'aside-menu-show',
    'aside-menu-sm-show',
    'aside-menu-md-show',
    'aside-menu-lg-show',
    'aside-menu-xl-show'
  ];
